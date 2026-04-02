import os
from contextlib import asynccontextmanager
from typing import Dict

import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://counter:counter@localhost:5432/counter",
)


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
    app.state.db = pool
    yield
    await pool.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


class ApplyRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    amount: int


class BalanceResponse(BaseModel):
    user_id: str
    balance: int


class BalancesResponse(BaseModel):
    balances: Dict[str, int]


@app.post("/apply", response_model=BalanceResponse)
async def apply_transaction(req: ApplyRequest) -> BalanceResponse:
    async with app.state.db.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO balances (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = balances.balance + EXCLUDED.balance
            RETURNING user_id, balance
            """,
            req.user_id,
            req.amount,
        )
    return BalanceResponse(user_id=row["user_id"], balance=row["balance"])


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
