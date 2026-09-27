from dataclasses import dataclass
from pydantic import BaseModel

import logging
import httpx

from .resolver import TxnFinal

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("normalize:firefly")

class TxnSplit(BaseModel):
    transaction_journal_id: str
    description: str
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
    description: str
    destination: str | None

    @classmethod
    def from_txn(cls, txn: dict) -> "TxnOriginal":
        return cls(description=txn.get("description", ""),
                   destination=txn.get("destination_name"))

    def render(self) -> str:
        return " ".join((
            ORIGINAL_HEAD,
            f"description: {self.description} "
            f"destination: {self.destination or ''}",
            ORIGINAL_TAIL))

    def merge_notes(self, notes: str | None) -> str:
        current = (notes or "").strip()
        if ORIGINAL_HEAD in current:
            return current
        return f"{current}{' '*10}{self.render()}" if current else self.render()


NORMALIZE_TAG = "normalized"


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

    @staticmethod
    def _merge_tags(tags: list[str] | None) -> list[str]:
        current = list(tags or [])
        if NORMALIZE_TAG not in current:
            current.append(NORMALIZE_TAG)
        return current

    async def update_transaction(self, group_id: int,
                                 splits: list[TxnSplit]) -> None:
        update = TransactionUpdate(transactions=splits)
        rpath = f"{self._url}/api/v1/transactions/{group_id}"
        response = await self._http.put(rpath,
                            json=update.model_dump(exclude_none=True),
                            headers=self._headers(), timeout=30)
        if response.status_code >= 400:
            log.error("Firefly update failed (%s): %s",
                      response.status_code, response.text)
            return
        
        response.raise_for_status()




