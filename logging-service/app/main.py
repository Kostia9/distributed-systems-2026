import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import hazelcast

HZ_ADDRESSES = [
    addr.strip()
    for addr in os.getenv("HZ_ADDRESSES", "localhost:5701").split(",")
    if addr.strip()
]
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://localhost:8003").rstrip("/")
SERVICE_URL = os.getenv("SERVICE_URL", "http://localhost:8001").rstrip("/")
SERVICE_NAME = "logging-service"
MAP_NAME = "transactions"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("logging-service")


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


async def _await_hz(hz_future):
    loop = asyncio.get_running_loop()
    aio_future = loop.create_future()

    def _set_result(value):
        if not aio_future.done():
            aio_future.set_result(value)

    def _set_exception(exc):
        if not aio_future.done():
            aio_future.set_exception(exc)

    def _on_done(f):
        try:
            result = f.result()
        except BaseException as exc:
            loop.call_soon_threadsafe(_set_exception, exc)
        else:
            loop.call_soon_threadsafe(_set_result, result)

    hz_future.add_done_callback(_on_done)
    return await aio_future


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = hazelcast.HazelcastClient(
        cluster_members=HZ_ADDRESSES,
        cluster_name="dev",
    )
    app.state.hz_client = client
    app.state.tx_map = client.get_map(MAP_NAME)
    await _register_service()
    logger.info("Connected to Hazelcast cluster, members: %s", HZ_ADDRESSES)
    yield
    client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: int


class TransactionList(BaseModel):
    transactions: list[Transaction]


@app.post("/transactions", response_model=Transaction)
async def add_transaction(tx: Transaction) -> Transaction:
    tx_map = app.state.tx_map
    payload = tx.model_dump_json()
    await _await_hz(tx_map.put(tx.transaction_id, payload))
    logger.info(
        "service=%s stored transaction %s (user=%s, amount=%d)",
        SERVICE_URL,
        tx.transaction_id,
        tx.user_id,
        tx.amount,
    )
    return tx


@app.get("/transactions", response_model=TransactionList)
async def get_all_transactions() -> TransactionList:
    tx_map = app.state.tx_map
    raw_values = await _await_hz(tx_map.values())
    items = [Transaction(**json.loads(v)) for v in raw_values]
    return TransactionList(transactions=items)


@app.get("/transactions/user/{user_id}", response_model=TransactionList)
async def get_user_transactions(user_id: str) -> TransactionList:
    tx_map = app.state.tx_map
    raw_values = await _await_hz(tx_map.values())
    items = [
        Transaction(**d)
        for d in (json.loads(v) for v in raw_values)
        if d["user_id"] == user_id
    ]
    return TransactionList(transactions=items)


@app.get("/transactions/{transaction_id}", response_model=Transaction)
async def get_transaction(transaction_id: str) -> Transaction:
    tx_map = app.state.tx_map
    raw = await _await_hz(tx_map.get(transaction_id))
    if raw is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return Transaction(**json.loads(raw))


@app.post("/reset")
async def reset() -> dict[str, bool]:
    tx_map = app.state.tx_map
    await _await_hz(tx_map.clear())
    logger.info("Cleared Hazelcast map %s", MAP_NAME)
    return {"ok": True}
