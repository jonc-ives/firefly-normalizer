from dataclasses import dataclass
from pydantic import BaseModel
from typing import TYPE_CHECKING

import logging
import httpx

from .resolver import TxnFinal

if TYPE_CHECKING:
    from .transfer import TxnTransfer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("normalize:firefly")

class TxnSplit(BaseModel):
    transaction_journal_id: str
    description: str
    type: str | None = None
    date: str | None = None
    source_id: str | None = None
    destination_id: str | None = None
    destination_name: str | None = None
    notes: str | None = None
    tags: list[str] | None = None


class TransactionUpdate(BaseModel):
    transactions: list[TxnSplit]
    apply_rules: bool = False
    fire_webhooks: bool = False


ORIGINAL_HEAD = "[NORMALIZE"
ORIGINAL_TAIL = "END]"


@dataclass(frozen=True)
class TxnOriginal:
    journal_id: str
    description: str
    source: str | None
    destination: str | None
    external_id: str | None

    @classmethod
    def from_txn(cls, txn: dict) -> "TxnOriginal":
        return cls(journal_id=str(txn["transaction_journal_id"]),
                   description=txn.get("description", ""),
                   source=txn.get("source_name"),
                   destination=txn.get("destination_name"),
                   external_id=txn.get("external_id"))
    
    def render(self) -> str:
        return " ".join((
            ORIGINAL_HEAD,
            f"journal: {self.journal_id} ",
            f"description: {self.description} ",
            f"source: {self.source or ''}",
            f"destination: {self.destination or ''}",
            f"external_id: {self.external_id or ''}",
            ORIGINAL_TAIL))

    def merge_notes(self, notes: str | None) -> str:
        current = (notes or "").strip()
        if f"journal: {self.journal_id} " in current:
            return current
        return f"{current} {self.render()}" if current else self.render()


NORMALIZE_TAG = "normalized"
TRANSFER_TAG = "transfer-matched"
PENDING_TAG = "transfer-pending"


class FireflyClient:
    def __init__(self, http: httpx.AsyncClient, url: str, key: str):
        self._http = http
        self._url = url
        self._key = key

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/json"
        }

    @staticmethod
    def _flatten(data: dict) -> dict:
        group_id = int(data["id"])
        splits = data["attributes"]["transactions"]
        for split in splits:
            split["group_id"] = group_id
        return {"id": group_id, "transactions": splits}

    @classmethod
    def _build_split(cls, txn: dict, final: TxnFinal) -> TxnSplit:
        tjid = str(txn["transaction_journal_id"])
        dest = final.merchant if txn.get("type") == "withdrawal" else None
        orig = TxnOriginal.from_txn(txn)
        notes = orig.merge_notes(txn.get("notes"))
        tags = cls._merge_tags(txn.get("tags"))
        return TxnSplit(transaction_journal_id=tjid,
                description=final.description,
                destination_name=dest,
                notes=notes,
                tags=tags)

    @classmethod
    def _build_pending_split(cls, txn: dict) -> TxnSplit:
        tjid = str(txn["transaction_journal_id"])
        tags = cls._merge_tags(txn.get("tags"), add=(PENDING_TAG,))
        return TxnSplit(transaction_journal_id=tjid, tags=tags)

    @classmethod
    def _build_transfer_split(cls, final: "TxnTransfer") -> TxnSplit:
        to_survive, to_delete = final.to_survive, final.to_delete
        tjid = str(to_survive["transaction_journal_id"])
        notes = TxnOriginal.from_txn(
            to_survive).merge_notes(to_survive.get("notes"))
        notes = TxnOriginal.from_txn(to_delete).merge_notes(notes)
        tags = [*(to_survive.get("tags", [])), *(to_delete.get("tags", []))]
        tags = cls._merge_tags(tags, add=(TRANSFER_TAG,), drop=(PENDING_TAG,))
        return TxnSplit(transaction_journal_id=tjid,
                        type="transfer",
                        date=final.date,
                        source_id=final.source_id,
                        destination_id=final.destination_id,
                        description=final.description,
                        notes=notes,
                        tags=tags)

    @staticmethod
    def _merge_tags(tags:list[str] | None,
        add: tuple[str, ...] = (NORMALIZE_TAG,),
        drop: tuple[str, ...] = ()
    ) -> list[str]:
        current = [t for t in dict.fromkeys(tags or []) if t not in drop]
        for tag in add:
            if tag not in current:
                current.append(tag)
        return current

    async def get_group(self, group_id: int) -> dict | None:
        rpath = f"{self._url}/api/v1/transactions/{group_id}"
        response = await self._http.get(rpath, headers=self._headers())
        if response.status_code == 404:
            return
        if response.status_code >= 400:
            log.error("firefly fetch failed (%s) : %s",
                      response.status_code, response.text)
        response.raise_for_status()
        return self._flatten(response.json()["data"])

    async def search(self, query: str, limit: int = 50) -> list[dict]:
        rpath = f"{self._url}/api/v1/search/transactions"
        pars = {"query": query, "limit": limit}        
        head = self._headers()
        response = await self._http.get(rpath, params=pars, headers=head)
        if response.status_code >= 400:
            log.error("firefly fetch failed (%s) : %s",
                      response.status_code, response.text)
        response.raise_for_status()
        return [self._flatten(d) for d in response.json()["data"]]

    async def update_transaction(self, group_id: int,
                                 splits: list[TxnSplit]) -> None:
        update = TransactionUpdate(transactions=splits)
        rpath = f"{self._url}/api/v1/transactions/{group_id}"
        response = await self._http.put(rpath,
                            json=update.model_dump(exclude_none=True),
                            headers=self._headers(), timeout=30)
        if response.status_code >= 400:
            log.error("firefly update failed (%s): %s",
                      response.status_code, response.text)
        response.raise_for_status()

    async def delete_group(self, group_id: int) -> None:
        rpath = f"{self._url}/api/v1/transactions/{group_id}"
        response = await self._http.delete(rpath, headers=self._headers())
        if response.status_code == 404:
            return
        if response.status_code >= 400:
            log.error("firefly delete failed (%s): %s",
                      response.status_code, response.text)
        response.raise_for_status()
