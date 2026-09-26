from pydantic import BaseModel

import logging
import httpx

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("normalize:firefly")

class TxnSplit(BaseModel):
    transaction_journal_id: str
    description: str
    destination_name: str | None = None


class TransactionUpdate(BaseModel):
    transactions: list[TxnSplit]
    apply_rules: bool = False
    fire_webhooks: bool = False


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




