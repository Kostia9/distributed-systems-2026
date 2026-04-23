from collections import defaultdict

from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="config-server")

_REGISTRY: dict[str, set[str]] = defaultdict(set)


class RegisterRequest(BaseModel):
    service_name: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)


class ServiceResponse(BaseModel):
    service_name: str
    urls: list[str]


@app.post("/register", response_model=ServiceResponse)
async def register_service(req: RegisterRequest) -> ServiceResponse:
    _REGISTRY[req.service_name].add(req.url)
    return ServiceResponse(
        service_name=req.service_name,
        urls=sorted(_REGISTRY[req.service_name]),
    )


@app.get("/services/{service_name}", response_model=ServiceResponse)
async def get_service(service_name: str) -> ServiceResponse:
    return ServiceResponse(
        service_name=service_name,
        urls=sorted(_REGISTRY.get(service_name, set())),
    )


@app.post("/reset")
async def reset_registry() -> dict[str, bool]:
    _REGISTRY.clear()
    return {"ok": True}
