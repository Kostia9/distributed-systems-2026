Microservices: Kubernetes Service Discovery + Config

This project uses Kubernetes as the service registry, service discovery layer,
and config server.

Services:
- `facade-service`: HTTP entrypoint. Calls downstream services through
  Kubernetes Service DNS and writes transaction messages to Hazelcast Queue.
- `logging-service`: stores transactions in Hazelcast Map.
- `counter-service`: consumes transaction messages from Hazelcast Queue and
  applies balances to PostgreSQL.

Infrastructure:
- Hazelcast cluster: 3 Kubernetes pods in a StatefulSet.
- Hazelcast Management Center: optional UI at http://localhost:8080 through
  port-forwarding.
- PostgreSQL 17: Kubernetes Deployment, Service, PVC, and Secret.

Kubernetes responsibilities:
- Service registration: pods are registered behind Kubernetes Services by label
  selectors.
- Service discovery: `facade-service` uses Service DNS names from `app-config`.
- Config server: Hazelcast, queue, and downstream service settings live in
  `ConfigMap`; PostgreSQL credentials live in `Secret`.
- Failover: Services route calls only to ready pods, and endpoints change when
  pods are deleted or become unavailable.

## Kubernetes manifests

Manifests are in `k8s/`:
- `app-config.yaml`: application `ConfigMap` and PostgreSQL `Secret`.
- `postgres.yaml`: PostgreSQL PVC, Deployment, and Service.
- `hazelcast.yaml`: Hazelcast ConfigMap, StatefulSet, Services, and Management
  Center.
- `microservices.yaml`: Deployments and Services for facade, logging, counter.

## Run in Minikube

The `Makefile` wraps the full lifecycle. Run `make help` for the target list.

```bash
make minikube-start    # one-time per session
make up                # build images + apply manifests + wait for rollouts
make pf                # port-forward facade to :8000
make smoke             # curl-based functional check (alice balance == 120)
make perf              # perf_test scenario 1 (10 clients × 1000 requests)
make perf2             # perf_test scenario 2
make down              # delete manifests and PVCs
```

Useful inspection targets:

```bash
make status            # kubectl get pods,svc,statefulset,deploy
make pods              # watch pods (kubectl get pods -w)
make logs              # tail logs of all 3 microservices
make logs-facade       # tail facade only
make logs-hz           # tail hazelcast-0
make pf-mc             # port-forward Management Center to :8080
```

Underneath, `make up` runs:

1. `eval $(minikube docker-env)` and `docker build -t distributed-<svc>:latest ./<svc>` for the three microservices.
2. `kubectl apply -f k8s/{app-config,postgres,hazelcast,microservices}.yaml`.
3. `kubectl rollout status` for the Hazelcast StatefulSet, PostgreSQL, and
   the three microservice Deployments.

Inspect registered instances and service endpoints (Kubernetes-native discovery):

```bash
kubectl get endpoints logging-service counter-service facade-service
kubectl get endpointslices -l kubernetes.io/service-name=logging-service
```

With `make pf` running, exercise the API:

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
```

## Scale and failover demo

The current Kubernetes manifests start `logging-service` with 3 replicas,
`facade-service` with 1 replica, and `counter-service` with 1 replica.

```bash
kubectl get pods -l app=logging-service
kubectl get endpoints logging-service
```

With `make pf` running in one terminal and `make logs-logging` in another, delete one logging pod and watch Kubernetes remove it from endpoints and reschedule a replacement:

```bash
POD=$(kubectl get pods -l app=logging-service \
  -o jsonpath='{.items[0].metadata.name}')

kubectl delete pod "$POD"
kubectl get pods -l app=logging-service
kubectl get endpoints logging-service
```

Calls continue through the `logging-service` Kubernetes Service:

```bash
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8000/transaction \
    -H "content-type: application/json" \
    -d '{"user_id":"u2","amount":2}'
  echo
done

curl http://localhost:8000/user/u2
```
