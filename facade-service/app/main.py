import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException, Request, Depends
from pydantic import BaseModel, Field

# --- Constants ---
LOGGING_URL = os.getenv("LOGGING_URL", "http://localhost:8001").rstrip("/")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002").rstrip("/")
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))


# --- Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    )
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT_S),
        limits=limits,
    ) as client:
        app.state.http_client = client
        yield


app = FastAPI(title="facade-service", lifespan=lifespan)


def get_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


# --- Request models ---
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


# --- Metrics ---
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
    """Measure and record timing for a coroutine."""
    t0 = perf_counter()
    try:
        return await coro
    finally:
        dt = perf_counter() - t0
        _TIMINGS[name].add(dt)


# --- Endpoints ---
@app.post("/transaction", response_model=FacadePostResponse)
async def post_transaction(
    req: ClientTransactionIn, client: httpx.AsyncClient = Depends(get_client)
) -> FacadePostResponse:
    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "amount": req.amount,
    }
    log_request = client.post(f"{LOGGING_URL}/transactions", json=tx)
    counter_request = client.post(
        f"{COUNTER_URL}/apply", json={"user_id": req.user_id, "amount": req.amount}
    )
    try:
        log_resp, counter_resp = await asyncio.gather(
            _timed("logging", log_request),
            _timed("counter", counter_request),
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"downstream unavailable: {e!s}")

    # error handling
    if log_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"logging-service error: {log_resp.text}"
        )
    if counter_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"counter-service error: {counter_resp.text}"
        )

    return FacadePostResponse(
        transaction_id=tx["transaction_id"], balance=int(counter_resp.json()["balance"])
    )


@app.get("/user/{user_id}", response_model=UserViewResponse)
async def get_user_view(
    user_id: str, client: httpx.AsyncClient = Depends(get_client)
) -> UserViewResponse:
    try:
        bal_resp, txs_resp = await asyncio.gather(
            _timed("counter", client.get(f"{COUNTER_URL}/balance/{user_id}")),
            _timed("logging", client.get(f"{LOGGING_URL}/transactions/user/{user_id}")),
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"downstream unavailable: {e!s}")

    # error handling
    if bal_resp.status_code >= 400 or txs_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="Downstream service error")

    return UserViewResponse(
        user_id=user_id,
        balance=int(bal_resp.json()["balance"]),
        transactions=txs_resp.json().get("transactions", []),
    )


@app.get("/accounts", response_model=AccountsResponse)
async def get_accounts(
    client: httpx.AsyncClient = Depends(get_client),
) -> AccountsResponse:
    try:
        resp = await _timed("counter", client.get(f"{COUNTER_URL}/balances"))
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"downstream unavailable: {e!s}")

    # error handling
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
