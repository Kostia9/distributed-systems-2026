import asyncio
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


LOGGING_URL = os.getenv("LOGGING_URL", "http://localhost:8001").rstrip("/")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002").rstrip("/")
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))

app = FastAPI(title="facade-service")


class ClientTransactionIn(BaseModel):
    user_id: str = Field(..., min_length=1)
    amount: int


class FacadePostResponse(BaseModel):
    transaction_id: str
    balance: int


class UserViewResponse(BaseModel):
    user_id: str
    balance: int
    transactions: List[Dict[str, Any]]


class AccountsResponse(BaseModel):
    balances: Dict[str, int]


@dataclass
class Timing:
    total_s: float = 0.0
    count: int = 0

    def add(self, dt_s: float) -> None:
        self.total_s += dt_s
        self.count += 1

    def snapshot(self) -> Dict[str, Any]:
        avg_ms = (self.total_s / self.count * 1000.0) if self.count else 0.0
        return {"count": self.count, "total_s": self.total_s, "avg_ms": avg_ms}


_TIMINGS: Dict[str, Timing] = {
    "logging": Timing(),
    "counter": Timing(),
}


async def _timed(name: str, coro):
    t0 = perf_counter()
    try:
        return await coro
    finally:
        dt = perf_counter() - t0
        _TIMINGS[name].add(dt)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _client() -> httpx.AsyncClient:
    # A small wrapper so we can tweak defaults centrally.
    return httpx.AsyncClient(timeout=httpx.Timeout(HTTP_TIMEOUT_S))


@app.post("/transaction", response_model=FacadePostResponse)
async def post_transaction(req: ClientTransactionIn) -> FacadePostResponse:
    # Spec allows timestamp as unique ID; we use UUID (still include timestamp).
    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "amount": req.amount,
        "timestamp": _utc_now().isoformat(),
    }

    async with await _client() as client:
        # Send to both services concurrently; wait for both.
        try:
            log_resp, counter_resp = await asyncio.gather(
                _timed("logging", client.post(f"{LOGGING_URL}/transactions", json=tx)),
                _timed(
                    "counter",
                    client.post(
                        f"{COUNTER_URL}/apply",
                        json={"user_id": req.user_id, "amount": req.amount},
                    ),
                ),
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, detail=f"downstream unavailable: {e!s}"
            )

    if log_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"logging-service error: {log_resp.text}"
        )
    if counter_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"counter-service error: {counter_resp.text}"
        )

    data = counter_resp.json()
    return FacadePostResponse(
        transaction_id=tx["transaction_id"], balance=int(data["balance"])
    )


@app.get("/user/{user_id}", response_model=UserViewResponse)
async def get_user_view(user_id: str) -> UserViewResponse:
    async with await _client() as client:
        try:
            bal_resp, txs_resp = await asyncio.gather(
                _timed("counter", client.get(f"{COUNTER_URL}/balance/{user_id}")),
                _timed(
                    "logging", client.get(f"{LOGGING_URL}/transactions/user/{user_id}")
                ),
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, detail=f"downstream unavailable: {e!s}"
            )

    if bal_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"counter-service error: {bal_resp.text}"
        )
    if txs_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"logging-service error: {txs_resp.text}"
        )

    balance = int(bal_resp.json()["balance"])
    txs = txs_resp.json().get("transactions", [])
    return UserViewResponse(user_id=user_id, balance=balance, transactions=txs)


@app.get("/accounts", response_model=AccountsResponse)
async def get_accounts() -> AccountsResponse:
    async with await _client() as client:
        try:
            resp = await _timed("counter", client.get(f"{COUNTER_URL}/balances"))
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, detail=f"downstream unavailable: {e!s}"
            )

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"counter-service error: {resp.text}"
        )

    return AccountsResponse(balances=resp.json().get("balances", {}))


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    return {
        "logging": _TIMINGS["logging"].snapshot(),
        "counter": _TIMINGS["counter"].snapshot(),
    }


@app.post("/metrics/reset")
async def reset_metrics() -> Dict[str, Any]:
    _TIMINGS["logging"] = Timing()
    _TIMINGS["counter"] = Timing()
    return {"ok": True}
