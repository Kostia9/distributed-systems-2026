import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Annotated, Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

import hazelcast

SERVICE_NAME = "facade-service"
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("facade-service")


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required")
    return value


def _required_csv_env(name: str) -> list[str]:
    values = [item.strip() for item in _required_env(name).split(",") if item.strip()]
    if not values:
        raise RuntimeError(f"{name} environment variable must not be empty")
    return values


HZ_ADDRESSES = _required_csv_env("HZ_ADDRESSES")
HZ_CLUSTER_NAME = _required_env("HZ_CLUSTER_NAME")
QUEUE_NAME = _required_env("COUNTER_QUEUE_NAME")
SERVICE_URLS = {
    "logging-service": _required_env("LOGGING_SERVICE_URL").rstrip("/"),
    "counter-service": _required_env("COUNTER_SERVICE_URL").rstrip("/"),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    )
    hz_client = hazelcast.HazelcastClient(
        cluster_members=HZ_ADDRESSES,
        cluster_name=HZ_CLUSTER_NAME,
    )
    counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()
    logger.info("Connected to Hazelcast cluster, members: %s", HZ_ADDRESSES)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT_S),
        limits=limits,
    ) as client:
        app.state.http_client = client
        app.state.hz_client = hz_client
        app.state.counter_queue = counter_queue
        yield
    hz_client.shutdown()


app = FastAPI(title="facade-service", lifespan=lifespan)


def get_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


HttpClientDependency = Annotated[httpx.AsyncClient, Depends(get_client)]


# --- Request models ---
class ClientTransactionIn(BaseModel):
    user_id: str = Field(..., min_length=1)
    amount: int = Field(..., ge=INT64_MIN, le=INT64_MAX)


class FacadePostResponse(BaseModel):
    transaction_id: str
    status: str


class UserViewResponse(BaseModel):
    user_id: str
    balance: int | None
    transactions: list[dict[str, Any]]


class AccountsResponse(BaseModel):
    balances: dict[str, int] | None


# --- Metrics ---
@dataclass
class Timing:
    total_s: float = 0.0
    count: int = 0

    def add(self, dt_s: float) -> None:
        self.total_s += dt_s
        self.count += 1

    def snapshot(self) -> dict[str, Any]:
        avg_ms = (self.total_s / self.count * 1000.0) if self.count else 0.0
        return {"count": self.count, "total_s": self.total_s, "avg_ms": avg_ms}


_TIMINGS: dict[str, Timing] = {
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


async def _service_request(
    client: httpx.AsyncClient,
    service_name: str,
    method: str,
    path: str,
    **kwargs,
) -> httpx.Response:
    last_err: httpx.RequestError | None = None
    url = SERVICE_URLS[service_name]
    for _ in range(2):
        try:
            return await client.request(method, f"{url}{path}", **kwargs)
        except httpx.RequestError as exc:
            last_err = exc
    raise HTTPException(
        status_code=503,
        detail=f"{service_name} unavailable: {last_err!s}"
        if last_err
        else f"{service_name} unavailable",
    )


# --- Endpoints ---
@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/transaction", response_model=FacadePostResponse, status_code=202)
async def post_transaction(
    req: ClientTransactionIn,
    request: Request,
    client: HttpClientDependency,
) -> FacadePostResponse:
    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": req.user_id,
        "amount": req.amount,
    }
    try:
        log_resp = await _timed(
            "logging",
            _service_request(
                client, "logging-service", "POST", "/transactions", json=tx
            ),
        )
        if log_resp.status_code >= 400:
            raise HTTPException(
                status_code=502, detail=f"logging-service error: {log_resp.text}"
            )
        await _timed(
            "counter",
            asyncio.to_thread(request.app.state.counter_queue.put, json.dumps(tx)),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"message queue unavailable: {exc!s}",
        ) from exc

    return FacadePostResponse(transaction_id=tx["transaction_id"], status="queued")


@app.get("/user/{user_id}", response_model=UserViewResponse)
async def get_user_view(user_id: str, client: HttpClientDependency) -> UserViewResponse:
    balance: int | None = None

    txs_resp = await _timed(
        "logging",
        _service_request(
            client, "logging-service", "GET", f"/transactions/user/{user_id}"
        ),
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
        raise HTTPException(
            status_code=502, detail=f"logging-service error: {txs_resp.text}"
        )
    if bal_resp is not None:
        if bal_resp.status_code >= 400:
            raise HTTPException(
                status_code=502, detail=f"counter-service error: {bal_resp.text}"
            )
        balance = int(bal_resp.json()["balance"])

    return UserViewResponse(
        user_id=user_id,
        balance=balance,
        transactions=txs_resp.json().get("transactions", []),
    )


@app.get("/accounts", response_model=AccountsResponse)
async def get_accounts(
    client: HttpClientDependency,
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
        raise HTTPException(
            status_code=502, detail=f"counter-service error: {resp.text}"
        )

    return AccountsResponse(balances=resp.json().get("balances", {}))


def _facade_metrics_snapshot() -> dict[str, Any]:
    return {
        "logging": _TIMINGS["logging"].snapshot(),
        "counter": _TIMINGS["counter"].snapshot(),
    }


@app.get("/metrics")
async def get_metrics() -> dict[str, Any]:
    return _facade_metrics_snapshot()


@app.post("/metrics/reset")
async def reset_metrics() -> dict[str, Any]:
    _TIMINGS["logging"] = Timing()
    _TIMINGS["counter"] = Timing()
    return {"ok": True}


@app.get("/metrics/all")
async def get_metrics_all(client: HttpClientDependency) -> dict[str, Any]:
    counter_metrics: dict[str, Any] = {}
    try:
        resp = await _service_request(client, "counter-service", "GET", "/metrics")
        if resp.status_code < 400:
            counter_metrics = resp.json()
    except HTTPException:
        pass
    return {"facade": _facade_metrics_snapshot(), "counter": counter_metrics}


@app.post("/metrics/reset/all")
async def reset_metrics_all(client: HttpClientDependency) -> dict[str, Any]:
    _TIMINGS["logging"] = Timing()
    _TIMINGS["counter"] = Timing()
    counter_ok = False
    try:
        resp = await _service_request(
            client, "counter-service", "POST", "/metrics/reset"
        )
        counter_ok = resp.status_code < 400
    except HTTPException:
        pass
    return {"facade": True, "counter": counter_ok}


@app.post("/reset")
async def reset_state(
    request: Request,
    client: HttpClientDependency,
) -> dict[str, Any]:
    await asyncio.to_thread(request.app.state.counter_queue.clear)

    counter_resp = await _service_request(client, "counter-service", "POST", "/reset")
    if counter_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"counter reset failed: {counter_resp.text}"
        )

    logging_resp = await _service_request(client, "logging-service", "POST", "/reset")
    if logging_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"logging reset failed: {logging_resp.text}"
        )

    return {"counter": True, "logging": True, "queue": True}
