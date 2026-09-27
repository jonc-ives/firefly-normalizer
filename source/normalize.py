""""""
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

import hashlib
import hmac
import httpx
import json
import logging
import os

from .webhook import WebhookFeature
from .firefly import TxnSplit, FireflyClient
from .resolver import TxnFinal, TransactionResolver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

log = logging.getLogger("normalize:normalize")

@dataclass(frozen=True)
class NormalizeConfig:
    firefly_url: str
    firefly_key: str
    webhook_key: str
    ollama_model: str
    ollama_url: str

    @staticmethod
    def _from_env(env: str, default: Any = None) -> Any:        
        var = os.environ.get(env, default)
        if var is None and default is None:
            raise ValueError(f"missing required environment {env!r}")
        return var

    @classmethod
    def from_env(cls) -> "NormalizeConfig":
        return cls (
            firefly_url=cls._from_env("FIREFLY_URL").rstrip("/"),
            firefly_key=cls._from_env("FIREFLY_KEY"),
            webhook_key=cls._from_env("WEBHOOK_KEY"),
            ollama_model=cls._from_env("OLLAMA_MODEL"),
            ollama_url=cls._from_env("OLLAMA_URL")
        )


class TransactionProcessor:
    def __init__(self, rslv: TransactionResolver, ffly: FireflyClient):
        self._resolver = rslv
        self._firefly = ffly

    async def _try_resolve(self, raw: str) -> TxnFinal | None:
        try:
            final = await self._resolver.resolve(raw)
        except Exception as e:
            log.error("resolve failed for %r: %s", raw, e)
            final = None
        
        return final

    async def process(self, group: dict) -> None:
        group_id = group["id"]
        splits: list[TxnSplit] = []

        for txn in group.get("transactions", []):
            if not (raw := txn.get("description", "")):
                continue
            if not (final := await self._try_resolve(raw)):
                continue

            split = self._firefly._build_split(txn, final)
            splits.append(split)
            log.info("group %s: %r -> %s", group_id,
                     raw, split.model_dump(exclude_none=True))

        if not splits:
            return

        try:
            await self._firefly.update_transaction(group_id, splits)
        except Exception as e:
            log.error("update of group %s failed: %s", group_id, e)


class FeatureNormalize(WebhookFeature):
    def __verify_hmac(self,
        key: str,
        body: bytes,
        header: Optional[str]
    ) -> None:
        """"""
        if not header:
            raise HTTPException(status_code=401,
                detail="missing signature")    

        parts = dict(
            p.split("=", 1) for p in header.split(",") if "=" in p)
        sig = parts.get("v1")
        ts = parts.get("t")

        if not ts or not sig:
            raise HTTPException(status_code=401,
                detail="malformed signature")

        expected = hmac.new(key.encode(), f"{ts}.".encode() + body,
                        hashlib.sha3_256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(status_code=401, detail="bad signature")

    def install_feature(self, config: NormalizeConfig):
        self.config = config
        self.processor : Optional[TransactionProcessor] = None
        self.install_route("/transaction", self.receive, ["POST"])

    @asynccontextmanager
    async def lifespan(self, _) -> AsyncGenerator[None]:
        async with httpx.AsyncClient(timeout=30) as http:
            ffly = FireflyClient(http, self.config.firefly_url,
                        self.config.firefly_key)
            rslv = TransactionResolver(http, self.config.ollama_url,
                        self.config.ollama_model)
            self.processor = TransactionProcessor(rslv, ffly)
            yield

        self.processor = None

    async def receive(self, req: Request, bg: BackgroundTasks) -> dict:
        """"""
        body = await req.body()
        wh_key = self.config.webhook_key
        rq_sig = req.headers.get("Signature")
        self.__verify_hmac(wh_key, body, rq_sig)

        payload = json.loads(body)
        if payload.get("trigger") != "STORE_TRANSACTION":
            return {"status": "ignored"}

        bg.add_task(self.processor.process, payload["content"])
        return {"status": "queued"}
