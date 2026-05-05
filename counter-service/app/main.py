import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from time import perf_counter

import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel

import hazelcast

SERVICE_NAME = "counter-service"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("counter-service")


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


DATABASE_URL = _required_env("DATABASE_URL")
HZ_ADDRESSES = _required_csv_env("HZ_ADDRESSES")
HZ_CLUSTER_NAME = _required_env("HZ_CLUSTER_NAME")
QUEUE_NAME = _required_env("COUNTER_QUEUE_NAME")


@dataclass
class Timing:
    total_s: float = 0.0
    count: int = 0

    def add(self, dt_s: float) -> None:
        self.total_s += dt_s
        self.count += 1

    def snapshot(self) -> dict[str, float | int]:
        avg_ms = (self.total_s / self.count * 1000.0) if self.count else 0.0
        return {"count": self.count, "total_s": self.total_s, "avg_ms": avg_ms}


_TIMINGS: dict[str, Timing] = {
    "db_apply": Timing(),
}


async def _timed(name: str, coro):
    t0 = perf_counter()
    try:
        return await coro
    finally:
        _TIMINGS[name].add(perf_counter() - t0)


async def _apply_transaction(pool: asyncpg.Pool, payload: str) -> None:
    tx = json.loads(payload)
    async with pool.acquire() as conn:
        await conn.fetchrow(
            """
            INSERT INTO balances (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = balances.balance + EXCLUDED.balance
            RETURNING user_id, balance
            """,
            tx["user_id"],
            tx["amount"],
        )
    logger.info(
        "Applied queued transaction %s (user=%s, amount=%d)",
        tx["transaction_id"],
        tx["user_id"],
        tx["amount"],
    )


async def _consume_transactions(app: FastAPI) -> None:
    queue = app.state.tx_queue
    while True:
        try:
            payload = await asyncio.to_thread(queue.poll, 1.0)
        except Exception:
            logger.exception("Failed to poll transaction queue, retrying")
            await asyncio.sleep(1)
            continue
        if payload is None:
            continue
        try:
            await _timed("db_apply", _apply_transaction(app.state.db, payload))
        except Exception:
            logger.exception(
                "Failed to apply queued transaction, returning it to the queue"
            )
            await asyncio.to_thread(queue.put, payload)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                user_id  VARCHAR(255) PRIMARY KEY,
                balance  BIGINT NOT NULL DEFAULT 0
            )
            """
        )
    hz_client = hazelcast.HazelcastClient(
        cluster_members=HZ_ADDRESSES,
        cluster_name=HZ_CLUSTER_NAME,
    )
    app.state.db = pool
    app.state.hz_client = hz_client
    app.state.tx_queue = hz_client.get_queue(QUEUE_NAME).blocking()
    app.state.consumer_task = asyncio.create_task(_consume_transactions(app))
    yield
    app.state.consumer_task.cancel()
    with suppress(asyncio.CancelledError):
        await app.state.consumer_task
    hz_client.shutdown()
    await pool.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


class BalanceResponse(BaseModel):
    user_id: str
    balance: int


class BalancesResponse(BaseModel):
    balances: dict[str, int]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/metrics")
async def get_metrics() -> dict[str, dict[str, float | int]]:
    return {"db_apply": _TIMINGS["db_apply"].snapshot()}


@app.post("/metrics/reset")
async def reset_metrics() -> dict[str, bool]:
    _TIMINGS["db_apply"] = Timing()
    return {"ok": True}


@app.get("/balance/{user_id}", response_model=BalanceResponse)
async def get_balance(user_id: str) -> BalanceResponse:
    async with app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM balances WHERE user_id = $1", user_id
        )
    return BalanceResponse(user_id=user_id, balance=row["balance"] if row else 0)


@app.get("/balances", response_model=BalancesResponse)
async def get_all_balances() -> BalancesResponse:
    async with app.state.db.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM balances")
    return BalancesResponse(balances={r["user_id"]: r["balance"] for r in rows})


@app.post("/reset")
async def reset() -> dict[str, bool]:
    async with app.state.db.acquire() as conn:
        await conn.execute("TRUNCATE balances")
    return {"ok": True}
