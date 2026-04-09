Microservices Basics (FastAPI): Hazelcast + PostgreSQL

Services:
- facade-service: client entrypoint (POST/GET); randomly picks a logging-service
  instance per request with failover to the next one on error.
- logging-service: stores transactions in a Hazelcast Distributed Map
  (`transactions`). Runs as 3 independent instances connected to the same
  Hazelcast cluster.
- counter-service: stores user balances in PostgreSQL (atomic upsert).

Infrastructure:
- Hazelcast cluster of 3 nodes (`hz1`, `hz2`, `hz3`) with TCP-IP discovery.
- Hazelcast Management Center at http://localhost:8080.
- PostgreSQL 17 with a named volume `pgdata` for persistence.

Run:
```bash
docker compose up --build -d
```

Ports:
- facade-service: 8000
- counter-service: 8002
- hz1/hz2/hz3: 5701/5702/5703
- management-center: 8080
- postgres: 5432

Facade API:
- POST /transaction — body: {"user_id":"u1","amount":10}
- GET  /user/{user_id}
- GET  /accounts
- POST /reset — clears Hazelcast map and truncates Postgres balances
- GET  /metrics
- POST /metrics/reset

Quick curl:
```bash
curl -X POST http://localhost:8000/transaction \
  -H "content-type: application/json" \
  -d '{"user_id":"u1","amount":10}'

curl http://localhost:8000/user/u1
curl http://localhost:8000/accounts
```

Verify distribution across logging-service instances via Hazelcast Management Center:
http://localhost:8080

Fault tolerance checks:
- Stop 1–2 logging-service instances — writes/reads still succeed via failover:
  ```bash
  docker compose stop logging-service-2 logging-service-3
  ```
- Stop 1–2 Hazelcast nodes — cluster keeps serving via remaining members:
  ```bash
  docker compose stop hz2 hz3
  ```
