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
RULE_TYPES = frozenset({"withdrawal", "deposit", "*"})


def _asset_id(txn: dict) -> str:
    return str(txn[f"{MAP_ASSET[txn['type']]}_id"])


def _date(txn: dict):
    return datetime.fromisoformat(txn["date"]).date()


def _amount(txn: dict) -> Decimal:
    return Decimal(txn["amount"])


@dataclass(frozen=True)
class TransferRule:
    """"""
    type: str
    pattern: re.Pattern

    @classmethod
    def from_entry(cls, index: int, entry: dict) -> "TransferRule":
        if not isinstance(entry, dict) or set(entry) != {"type", "pattern"}:
            raise ValueError(f"transfers[{index}]: expected {{type, pattern}}")
        if entry["type"] not in RULE_TYPES:
            raise ValueError(f"transfers[{index}]: type must be one of "
                             f"{sorted(RULE_TYPES)}")
        return cls(type=entry["type"],
                   pattern=re.compile(entry["pattern"], re.I))

    # def _side_ok(self, txn: dict) -> bool:
    #     if txn["type"] == "withdrawal":
    #         return txn.get("source_type") == ASSET
    #     if txn["type"] == "deposit":
    #         dest = txn.get("destination_type")
    #         if self.type == "de

    def matches(self, txn: dict) -> bool:
        if self.type not in ("*", txn["type"]):
            return False
        return bool(self.pattern.search(txn.get("description", "")))


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
        self._rules = self._load(patterns)

    @staticmethod
    def _load(path: str) -> tuple[TransferRule, ...]:
        if not path:
            return ()
        with open(path) as fp:
            entries = json.load(fp)
        if not isinstance(entries, list):
            raise ValueError(f"{path}: expected a JSON list")
        return tuple(
            TransferRule.from_entry(i, e) for i, e in enumerate(entries))

    def matches(self, txn: dict) -> bool:
        if txn.get("type") not in MAP_ASSET:
            return False
        return any(rule.matches(txn) for rule in self._rules)

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
    def _rank(txn: dict, groups: list[dict]) -> list[dict]:
        usable = [
            t for g in groups for t in g["transactions"]
            if t["type"] == MAP_WITHD[txn["type"]]
            and _amount(t) == _amount(txn)
            and t["currency_code"] == txn["currency_code"]
            and _asset_id(t) != _asset_id(txn)
        ]
        usable.sort(key=lambda c: abs((_date(c) - _date(txn)).days))
        return usable

    async def _verified(self, ranked: list[dict]) -> list[dict]:
        out = []
        for can in ranked:
            full = await self._firefly.get_group(can["group_id"])
            if full is None or len(full["transactions"]) != 1:
                continue
            out.append(full["transactions"][0])
            if len(out) == 2:
                break
        return out

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

    async def _mutual(self, txn: dict, cand: dict) -> bool:
        groups = await self._firefly.search(self._query(cand))
        back = await self._verified(self._rank(cand, groups))
        span = abs((_date(cand) - _date(txn)).days)
        for oth in back:
            if oth["transaction_journal_id"] == txn["transaction_journal_id"]:
                continue
            if abs((_date(cand) - _date(oth)).days) <= span:
                return False
        return True

    async def resolve(self, txn: dict) -> TxnTransfer | None:
        groups = await self._firefly.search(self._query(txn))
        found = await self._verified(self._rank(txn, groups))
        if not found:
            return None

        if len(found) > 1:
            span_a = abs((_date(found[0]) - _date(txn)).days)
            span_b = abs((_date(found[1]) - _date(txn)).days)
            if span_a == span_b:
                tid_a = found[0]["transaction_journal_id"]
                tid_b = found[1]["transaction_journal_id"]
                raise ValueError(f"ambiguous journals: {tid_a} and {tid_b}")

        if not await self._mutual(txn, found[0]):
            raise ValueError(f"journal {found[0]['transaction_journal_id']} "
                             f"has a closer or equal counterpart elsewhere")

        return self._plan(txn, found[0])