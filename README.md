Microservices Basics (FastAPI)

Three services:
- facade-service: client entrypoint (POST/GET), proxies to others
- logging-service: in-memory store of transactions (keyed by transaction_id)
- counter-service: in-memory balances (keyed by user_id)

Run:
 ```bash 
 docker compose up --build
```

Facade API:
- POST   /transaction              body: {"user_id":"u1","amount": 10}  (amount may be negative)
- GET    /user/{user_id}
- GET    /accounts
- GET    /metrics
- POST   /metrics/reset

Quick curl:
- ```bash
  curl -X POST http://localhost:8000/transaction -H "content-type: application/json" -d '{"user_id":"u1","amount": 10}'
  ```
- ```bash
  curl http://localhost:8000/user/u1
  ```
- ```bash
  curl http://localhost:8000/accounts
  ```
