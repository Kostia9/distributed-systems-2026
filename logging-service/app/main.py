import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import hazelcast

SERVICE_NAME = "logging-service"
MAP_NAME = "transactions"
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("logging-service")


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
INSTANCE_ID = os.getenv("HOSTNAME", SERVICE_NAME)


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
        cluster_name=HZ_CLUSTER_NAME,
    )
    app.state.hz_client = client
    app.state.tx_map = client.get_map(MAP_NAME)
    logger.info("Connected to Hazelcast cluster, members: %s", HZ_ADDRESSES)
    yield
    client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: int = Field(..., ge=INT64_MIN, le=INT64_MAX)


class TransactionList(BaseModel):
    transactions: list[Transaction]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/transactions", response_model=Transaction)
async def add_transaction(tx: Transaction) -> Transaction:
    tx_map = app.state.tx_map
    payload = tx.model_dump_json()
    await _await_hz(tx_map.put(tx.transaction_id, payload))
    logger.info(
        "instance=%s stored transaction %s (user=%s, amount=%d)",
        INSTANCE_ID,
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
