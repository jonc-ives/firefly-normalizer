""""""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import json
import logging
import re

from .firefly import FireflyClient, TRANSFER_TAG

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("normalize:transfer")

MAP_ASSET = {"withdrawal": "source", "deposit": "destination"}
MAP_WITHD = {"withdrawal": "deposit", "deposit": "withdrawal"}


def _asset_id(txn: dict) -> str:
    return str(txn[f"{MAP_ASSET[txn['type']]}_id"])


def _date(txn: dict):
    return datetime.fromisoformat(txn["date"]).date()


def _amount(txn: dict) -> Decimal:
    return Decimal(txn["amount"])


@dataclass(frozen=True)
class TxnTransfer:
    to_survive: dict
    to_delete: dict
    source_id: str
    destination_id: str
    description: str
    date: str

    @property
    def to_survive_group(self) -> int:
        return self.to_survive["group_id"]

    @property
    def to_delete_group(self) -> int:
        return self.to_delete["group_id"]


class TransferMatcher:
    def __init__(self, ffly: FireflyClient, patterns: str, window: int):
        self._firefly = ffly
        self._window = window
        self._patterns = tuple(
            re.compile(p, re.I) for p in self._load(patterns))

    @staticmethod
    def _load(path: str) -> list[str]:
        if not path:
            return []
        with open(path) as fp:
            return json.load(fp)

    def matches(self, description: str) -> bool:
        return any(p.search(description) for p in self._patterns)

    def _query(self, txn: dict) -> str:
        span = timedelta(days=self._window)
        return " ".join((
            f"type:{MAP_WITHD[txn['type']]}",
            f"amount:{txn['amount']}",
            f"currency_is:{txn['currency_code']}",
            f"date_after:{_date(txn) - span:%Y-%m-%d}",
            f"date_before:{_date(txn) + span:%Y-%m-%d}",
            f"-account_id:{_asset_id(txn)}",
            f"-tag_is:{TRANSFER_TAG}"
        ))

    @staticmethod
    def _select(txn: dict, groups: list[dict]) -> dict | None:
        """"""
        usable = [
            g["transactions"][0] for g in groups
            if len(g["transactions"]) == 1
            and g["transactions"][0]["type"] == MAP_WITHD[txn["type"]]
            and _amount(g["transactions"][0]) == _amount(txn)
            and g["transactions"][0]["currency_code"] == txn["currency_code"]
            and _asset_id(g["transactions"][0]) != _asset_id(txn)
        ]

        if not usable:
            return None

        usable.sort(key=lambda c: abs((_date(c) - _date(txn)).days))
        if len(usable > 1):
            span_recent = abs((_date(usable[0]) - _date(txn)).days)
            span_later = abs((_date(usable[1]) - _date(txn)).days)
            if span_recent == span_later:
                tid_a = usable[0]["transaction_journal_id"]
                tid_b = usable[1]["transaction_journal_id"]
                raise ValueError(f"ambiguous journals: {tid_a} and {tid_b}")

        return usable[0]
        
    @staticmethod
    def _plan(a: dict, b: dict) -> TxnTransfer:
        to_survive, to_delete = sorted((a, b), key=lambda t: t["group_id"])
        wd, dp = (a, b) if a["type"] == "withdrawal" else (b, a)
        return TxnTransfer(
            to_survive=to_survive,
            to_delete=to_delete,
            source_id=str(wd["source_id"]),
            destination_id=str(dp["destination_id"]),
            date=min(_date(a), _date(b)).isoformat(),
            description=f"Transfer: {wd['source_name']} -> "
                        f"{dp['destination_name']}"
        )

    async def resolve(self, txn: dict) -> TxnTransfer | None:
        groups = await self._firefly.search(self._query(txn))
        other = self._select(txn, groups)
        return self._plan(txn, other) if other else None
    