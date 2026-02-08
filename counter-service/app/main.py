from typing import Dict

from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="counter-service")

_BALANCES: Dict[str, int] = {}


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
    _BALANCES[req.user_id] = _BALANCES.get(req.user_id, 0) + req.amount
    bal = _BALANCES[req.user_id]
    return BalanceResponse(user_id=req.user_id, balance=bal)


@app.get("/balance/{user_id}", response_model=BalanceResponse)
async def get_balance(user_id: str) -> BalanceResponse:
    bal = _BALANCES.get(user_id, 0)
    return BalanceResponse(user_id=user_id, balance=bal)


@app.get("/balances", response_model=BalancesResponse)
async def get_all_balances() -> BalancesResponse:
    snapshot = dict(_BALANCES)
    return BalancesResponse(balances=snapshot)
