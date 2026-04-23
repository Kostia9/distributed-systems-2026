import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import Dict

import asyncpg
import hazelcast
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://counter:counter@localhost:5432/counter",
)
HZ_ADDRESSES = [addr.strip() for addr in os.getenv("HZ_ADDRESSES", "localhost:5701").split(",") if addr.strip()]
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://localhost:8003").rstrip("/")
SERVICE_URL = os.getenv("SERVICE_URL", "http://localhost:8002").rstrip("/")
SERVICE_NAME = "counter-service"
QUEUE_NAME = os.getenv("COUNTER_QUEUE_NAME", "counter-transactions")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("counter-service")


async def _register_service() -> None:
    payload = {"service_name": SERVICE_NAME, "url": SERVICE_URL}
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(f"{CONFIG_SERVER_URL}/register", json=payload)
                resp.raise_for_status()
            logger.info("Registered %s at %s", SERVICE_NAME, SERVICE_URL)
            return
        except httpx.HTTPError as exc:
            logger.warning("config-server unavailable, retrying registration: %s", exc)
            await asyncio.sleep(1)


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
        payload = await asyncio.to_thread(queue.poll, 1.0)
        if payload is None:
            continue
        try:
            await _apply_transaction(app.state.db, payload)
        except Exception:
            logger.exception("Failed to apply queued transaction, returning it to the queue")
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
        cluster_name="dev",
    )
    app.state.db = pool
    app.state.hz_client = hz_client
    app.state.tx_queue = hz_client.get_queue(QUEUE_NAME).blocking()
    await _register_service()
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
    balances: Dict[str, int]


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
async def reset() -> Dict[str, bool]:
    async with app.state.db.acquire() as conn:
        await conn.execute("TRUNCATE balances")
    return {"ok": True}
