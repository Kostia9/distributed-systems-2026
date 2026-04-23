import asyncio
import json
import logging
import os
import random
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict, List

import hazelcast
import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

SERVICE_NAME = "facade-service"
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://localhost:8003").rstrip("/")
SERVICE_URL = os.getenv("SERVICE_URL", "http://localhost:8000").rstrip("/")
HZ_ADDRESSES: List[str] = [addr.strip() for addr in os.getenv("HZ_ADDRESSES", "localhost:5701").split(",") if addr.strip()]
QUEUE_NAME = os.getenv("COUNTER_QUEUE_NAME", "counter-transactions")
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))

logger = logging.getLogger("facade-service")


async def _register_service(client: httpx.AsyncClient) -> None:
    payload = {"service_name": SERVICE_NAME, "url": SERVICE_URL}
    while True:
        try:
            resp = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload)
            resp.raise_for_status()
            logger.info("Registered %s at %s", SERVICE_NAME, SERVICE_URL)
            return
        except httpx.HTTPError as exc:
            logger.warning("config-server unavailable, retrying registration: %s", exc)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    )
    hz_client = hazelcast.HazelcastClient(
        cluster_members=HZ_ADDRESSES,
        cluster_name="dev",
    )
    counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT_S),
        limits=limits,
    ) as client:
        app.state.http_client = client
        app.state.hz_client = hz_client
        app.state.counter_queue = counter_queue
        await _register_service(client)
        yield
    hz_client.shutdown()


app = FastAPI(title="facade-service", lifespan=lifespan)


def get_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


# --- Request models ---
class ClientTransactionIn(BaseModel):
    user_id: str = Field(..., min_length=1)
    amount: int


class FacadePostResponse(BaseModel):
    transaction_id: str
    status: str


class UserViewResponse(BaseModel):
    user_id: str
    balance: int | None
    transactions: List[Dict[str, Any]]


class AccountsResponse(BaseModel):
    balances: Dict[str, int] | None


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


# --- Logging service failover helpers ---
async def _get_service_urls(
    client: httpx.AsyncClient,
    service_name: str,
) -> List[str]:
    try:
        resp = await client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"config-server unavailable: {exc!s}")

    urls = [url.rstrip("/") for url in resp.json().get("urls", []) if url]
    if not urls:
        raise HTTPException(status_code=503, detail=f"no instances registered for {service_name}")
    return urls


async def _service_request(
    client: httpx.AsyncClient,
    service_name: str,
    method: str,
    path: str,
    **kwargs,
) -> httpx.Response:
    urls = await _get_service_urls(client, service_name)
    urls = random.sample(urls, k=len(urls))
    last_err: httpx.RequestError | None = None
    for url in urls:
        try:
            return await client.request(method, f"{url}{path}", **kwargs)
        except httpx.RequestError as exc:
            last_err = exc
    raise HTTPException(
        status_code=503,
        detail=f"{service_name} unavailable: {last_err!s}" if last_err else f"{service_name} unavailable",
    )


# --- Endpoints ---
@app.post("/transaction", response_model=FacadePostResponse, status_code=202)
async def post_transaction(
    req: ClientTransactionIn,
    request: Request,
    client: httpx.AsyncClient = Depends(get_client),
) -> FacadePostResponse:
    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "amount": req.amount,
    }
    try:
        log_resp = await _timed(
            "logging",
            _service_request(client, "logging-service", "POST", "/transactions", json=tx),
        )
        if log_resp.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"logging-service error: {log_resp.text}")
        await _timed(
            "counter",
            asyncio.to_thread(request.app.state.counter_queue.put, json.dumps(tx)),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"message queue unavailable: {exc!s}")

    return FacadePostResponse(transaction_id=tx["transaction_id"], status="queued")


@app.get("/user/{user_id}", response_model=UserViewResponse)
async def get_user_view(
    user_id: str, client: httpx.AsyncClient = Depends(get_client)
) -> UserViewResponse:
    balance: int | None = None

    txs_resp = await _timed(
        "logging",
        _service_request(client, "logging-service", "GET", f"/transactions/user/{user_id}"),
    )
    try:
        bal_resp = await _timed(
            "counter",
            _service_request(client, "counter-service", "GET", f"/balance/{user_id}"),
        )
    except HTTPException as exc:
        if exc.status_code != 503:
            raise
        bal_resp = None

    if txs_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"logging-service error: {txs_resp.text}")
    if bal_resp is not None:
        if bal_resp.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"counter-service error: {bal_resp.text}")
        balance = int(bal_resp.json()["balance"])

    return UserViewResponse(
        user_id=user_id,
        balance=balance,
        transactions=txs_resp.json().get("transactions", []),
    )


@app.get("/accounts", response_model=AccountsResponse)
async def get_accounts(
    client: httpx.AsyncClient = Depends(get_client),
) -> AccountsResponse:
    try:
        resp = await _timed(
            "counter",
            _service_request(client, "counter-service", "GET", "/balances"),
        )
    except HTTPException as exc:
        if exc.status_code == 503:
            return AccountsResponse(balances=None)
        raise

    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"counter-service error: {resp.text}")

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


@app.post("/reset")
async def reset_state(
    request: Request,
    client: httpx.AsyncClient = Depends(get_client),
) -> Dict[str, Any]:
    await asyncio.to_thread(request.app.state.counter_queue.clear)

    counter_resp = await _service_request(client, "counter-service", "POST", "/reset")
    if counter_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"counter reset failed: {counter_resp.text}")

    logging_urls = await _get_service_urls(client, "logging-service")
    log_results = await asyncio.gather(
        *[client.post(f"{u}/reset") for u in logging_urls],
        return_exceptions=True,
    )
    logging_ok = any(not isinstance(r, Exception) and r.status_code < 400 for r in log_results)
    if not logging_ok:
        raise HTTPException(status_code=502, detail="logging reset failed on all instances")

    return {"counter": True, "logging": True, "queue": True}
