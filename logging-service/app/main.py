from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


app = FastAPI(title="logging-service")

_STORE: Dict[str, "Transaction"] = {}


class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: int


class TransactionList(BaseModel):
    transactions: List[Transaction]


@app.post("/transactions", response_model=Transaction)
async def add_transaction(tx: Transaction) -> Transaction:
    _STORE[tx.transaction_id] = tx
    return tx


@app.get("/transactions", response_model=TransactionList)
async def get_all_transactions() -> TransactionList:
    items = list(_STORE.values())
    return TransactionList(transactions=items)


@app.get("/transactions/user/{user_id}", response_model=TransactionList)
async def get_user_transactions(user_id: str) -> TransactionList:
    items = [t for t in _STORE.values() if t.user_id == user_id]
    return TransactionList(transactions=items)


@app.get("/transactions/{transaction_id}", response_model=Transaction)
async def get_transaction(transaction_id: str) -> Transaction:
    tx = _STORE.get(transaction_id)
    if tx is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return tx
