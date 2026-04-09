import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, List

import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

HZ_ADDRESSES = os.getenv("HZ_ADDRESSES", "localhost:5701").split(",")
MAP_NAME = "transactions"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("logging-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = hazelcast.HazelcastClient(
        cluster_members=HZ_ADDRESSES,
        cluster_name="dev",
    )
    app.state.hz_client = client
    app.state.tx_map = client.get_map(MAP_NAME).blocking()
    logger.info("Connected to Hazelcast cluster, members: %s", HZ_ADDRESSES)
    yield
    client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: int


class TransactionList(BaseModel):
    transactions: List[Transaction]


@app.post("/transactions", response_model=Transaction)
async def add_transaction(tx: Transaction) -> Transaction:
    tx_map = app.state.tx_map
    payload = tx.model_dump_json()
    await asyncio.to_thread(tx_map.put, tx.transaction_id, payload)
    logger.info("Stored transaction %s (user=%s, amount=%d)", tx.transaction_id, tx.user_id, tx.amount)
    return tx


@app.get("/transactions", response_model=TransactionList)
async def get_all_transactions() -> TransactionList:
    tx_map = app.state.tx_map
    raw_values = await asyncio.to_thread(tx_map.values)
    items = [Transaction(**json.loads(v)) for v in raw_values]
    return TransactionList(transactions=items)


@app.get("/transactions/user/{user_id}", response_model=TransactionList)
async def get_user_transactions(user_id: str) -> TransactionList:
    tx_map = app.state.tx_map
    raw_values = await asyncio.to_thread(tx_map.values)
    items = [
        Transaction(**d)
        for d in (json.loads(v) for v in raw_values)
        if d["user_id"] == user_id
    ]
    return TransactionList(transactions=items)


@app.get("/transactions/{transaction_id}", response_model=Transaction)
async def get_transaction(transaction_id: str) -> Transaction:
    tx_map = app.state.tx_map
    raw = await asyncio.to_thread(tx_map.get, transaction_id)
    if raw is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return Transaction(**json.loads(raw))


@app.post("/reset")
async def reset() -> Dict[str, bool]:
    tx_map = app.state.tx_map
    await asyncio.to_thread(tx_map.clear)
    logger.info("Cleared Hazelcast map %s", MAP_NAME)
    return {"ok": True}
