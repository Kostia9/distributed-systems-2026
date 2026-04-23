Microservices: Message Queue + Config Server

Services:
- `config-server`: in-memory registry of service URLs.
- `facade-service`: HTTP entrypoint for clients. Logs each transaction and pushes
  balance updates to Hazelcast Queue.
- `logging-service` x3: stores transactions in Hazelcast Map and registers itself
  in `config-server`.
- `counter-service`: reads queued transactions from Hazelcast Queue and applies
  them to PostgreSQL.

Infrastructure:
- Hazelcast cluster of 3 nodes: `hz1`, `hz2`, `hz3`
- Hazelcast Management Center: http://localhost:8080
- PostgreSQL 17: `localhost:5432`

Run:
```bash
docker compose up --build -d
```

Facade API:
- `POST /transaction` body: `{"user_id":"u1","amount":10}`
  Returns `202 Accepted` with `{"transaction_id":"...", "status":"queued"}`
- `GET /user/{user_id}`
- `GET /accounts`
- `POST /reset`
- `GET /metrics`
- `POST /metrics/reset`

Config API:
- `POST /register`
- `GET /services/{service_name}`
- `POST /reset`

Quick demo:
```bash
curl -X POST http://localhost:8000/reset

for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/transaction \
    -H "content-type: application/json" \
    -d '{"user_id":"u1","amount":1}'
  echo
done

curl http://localhost:8000/user/u1
curl http://localhost:8000/accounts
curl http://localhost:8003/services/logging-service
curl http://localhost:8003/services/counter-service
```

Show different logging instances in logs:
```bash
docker compose logs -f logging-service-1 logging-service-2 logging-service-3
```

Failure tolerance demo:
```bash
docker compose pause counter-service

curl -X POST http://localhost:8000/transaction \
  -H "content-type: application/json" \
  -d '{"user_id":"u1","amount":5}'

curl http://localhost:8000/user/u1
curl http://localhost:8000/accounts

docker compose unpause counter-service
sleep 5
curl http://localhost:8000/user/u1
```
