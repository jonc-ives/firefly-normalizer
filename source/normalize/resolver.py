from pydantic import BaseModel, Field
from dataclasses import dataclass

import httpx

SYSTEM_PROMPT = """You clean up messy financial transaction fields

Given a raw transaction description, return a JSON object with two fields:

- "merchant": this should description the destination of the transaction.
Purchases made to a company (e.g. Amazon) should use the brand name as
described by a person - no reference codes, no prefixes like "Debit Purchase",
no store numbers, no URLS, no city names unless inherently part of the brand.

- "description": A short, human-readable line. Usually the merchant name. Expand
abbreviations and add the product or service if the raw text makes it clear. This
should describe what the transaction was used for. 

Examples:

Kindle Unltd*ZF0C87YK3
{"merchant": "Kindle", "description": "Kindle Unlimited"}

Debit Purchase -visa Venmo *andrew Qunew York Ny 09/20 Card 7225
{"merchant": "Venmo", "description": "Venmo Payment"}

CIRCLEK #XXX7421 MELBOURNE FL
{"merchant": "CircleK", "description": "CircleK"}

Recurring Debit Purchase Anthropic Anthropic.coca 09/12 Card 7225
{"merchant": "Anthropic", "description": "Anthropic"}

Recurring Debit Purchase Netflix.com Netflix.com Ca 09/11 Card 7225
{"merchant": "Netflix", "description": "Netflix"}

Debit Purchase -visa Doordash*09/10-2855-431-0459ca 09/10 Card 7225
{"merchant": "Doordash", "description": "Doordash"}

Return only JSON."""


class ChatMessage(BaseModel):
    content: str
    role: str    


class ChatOptions(BaseModel):
    temperature: float = 0


class ChatRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    stream: bool = False
    think: bool = False
    format: dict = Field(
        default_factory=lambda: MerchantResponse.model_json_schema())
    options: ChatOptions = Field(default_factory=ChatOptions)


class MerchantResponse(BaseModel):
    merchant: str = Field(min_length=1)
    description: str = Field(min_length=1)


class ChatResponse(BaseModel):
    message: ChatMessage


@dataclass(frozen=True)
class TxnFinal:
    merchant: str
    description: str


class TransactionResolver:
    def __init__(self, http: httpx.AsyncClient, url: str, model: str):
        self._model = model
        self._http = http
        self._url = url

    def _messages(self, description: str) -> list[ChatMessage]:
        return [
            ChatMessage(role="system", content=SYSTEM_PROMPT),
            ChatMessage(role="user", content=description)
        ]

    async def resolve(self, description: str) -> TxnFinal:
        request = ChatRequest(
            model=self._model, messages=self._messages(description))
        
        rpath = f"{self._url}/api/chat"
        response = await self._http.post(
            rpath, json=request.model_dump(), timeout=120)
        response.raise_for_status()

        reply = ChatResponse.model_validate(response.json())
        result = MerchantResponse.model_validate_json(reply.message.content)
        return TxnFinal(merchant=result.merchant.strip(),
                        description=result.description.strip())




