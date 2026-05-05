SHELL    := /bin/bash
SERVICES := facade-service counter-service logging-service
IMAGE_PREFIX := distributed-
K8S_DIR  := k8s
FACADE_PORT := 8000
MC_PORT     := 8080

.PHONY: help up down restart build apply delete status pods logs logs-facade logs-counter logs-logging logs-hz wait reset smoke perf perf2 pf port-forward pf-mc minikube-start minikube-stop clean

help:
	@echo "Targets:"
	@echo "  make up              - build images + apply manifests + wait for rollout"
	@echo "  make down            - delete manifests and PVCs"
	@echo "  make restart         - down + up"
	@echo "  make build           - build service images into minikube docker"
	@echo "  make apply           - kubectl apply manifests (no build)"
	@echo "  make delete          - kubectl delete manifests"
	@echo "  make status          - kubectl get pods,svc"
	@echo "  make pods            - watch pods"
	@echo "  make logs            - tail all application logs"
	@echo "  make logs-facade     - tail facade-service logs"
	@echo "  make logs-counter    - tail counter-service logs"
	@echo "  make logs-logging    - tail logging-service logs"
	@echo "  make logs-hz         - tail hazelcast-0 logs"
	@echo "  make pf              - port-forward facade to localhost:$(FACADE_PORT)"
	@echo "  make port-forward    - alias for make pf"
	@echo "  make pf-mc           - port-forward management-center to localhost:$(MC_PORT)"
	@echo "  make smoke           - quick curl-based functional check (needs pf running)"
	@echo "  make reset           - POST /reset to facade (needs pf running)"
	@echo "  make perf            - run perf_test scenario 1 (needs pf running)"
	@echo "  make perf2           - run perf_test scenario 2 (needs pf running)"
	@echo "  make minikube-start  - start minikube"
	@echo "  make minikube-stop   - stop minikube"
	@echo "  make clean           - down + delete dangling images in minikube"

build:
	@echo ">>> Building images into minikube docker daemon"
	@eval $$(minikube docker-env) && \
	for s in $(SERVICES); do \
	 	echo ">>> docker build $$s"; \
		docker build -t $(IMAGE_PREFIX)$$s:latest ./$$s || exit 1; \
	done

apply:
	kubectl apply -f $(K8S_DIR)/app-config.yaml
	kubectl apply -f $(K8S_DIR)/postgres.yaml
	kubectl apply -f $(K8S_DIR)/hazelcast.yaml
	kubectl apply -f $(K8S_DIR)/microservices.yaml

wait:
	kubectl rollout status statefulset/hazelcast       --timeout=180s
	kubectl rollout status deploy/postgres             --timeout=120s
	kubectl rollout status deploy/logging-service      --timeout=120s
	kubectl rollout status deploy/counter-service      --timeout=120s
	kubectl rollout status deploy/facade-service       --timeout=120s
	@echo ">>> All workloads ready"

up: build apply wait
	@echo ""
	@echo ">>> Cluster is up. Next steps:"
	@echo "    make pf        # port-forward facade to localhost:$(FACADE_PORT)"
	@echo "    make smoke     # quick functional check"
	@echo "    make perf      # run perf_test"

delete:
	kubectl delete -f $(K8S_DIR)/microservices.yaml --ignore-not-found
	kubectl delete -f $(K8S_DIR)/hazelcast.yaml     --ignore-not-found
	kubectl delete -f $(K8S_DIR)/postgres.yaml      --ignore-not-found
	kubectl delete -f $(K8S_DIR)/app-config.yaml    --ignore-not-found

down: delete
	kubectl delete pvc --all --ignore-not-found
	@echo ">>> Cluster torn down"

restart: down up

status:
	kubectl get pods,svc,statefulset,deploy

pods:
	kubectl get pods -w

logs:
	kubectl logs -l 'app in (facade-service,counter-service,logging-service)' --tail=100 -f

logs-facade:
	kubectl logs -l app=facade-service --tail=100 -f

logs-counter:
	kubectl logs -l app=counter-service --tail=100 -f

logs-logging:
	kubectl logs -l app=logging-service --tail=100 -f

logs-hz:
	kubectl logs hazelcast-0 --tail=100 -f

pf:
	@echo ">>> facade-service on http://localhost:$(FACADE_PORT) (Ctrl-C to stop)"
	kubectl port-forward svc/facade-service $(FACADE_PORT):8000

port-forward: pf

pf-mc:
	@echo ">>> management-center on http://localhost:$(MC_PORT) (Ctrl-C to stop)"
	kubectl port-forward svc/management-center $(MC_PORT):8080

reset:
	curl -fsS -XPOST http://localhost:$(FACADE_PORT)/reset && echo

smoke:
	@set -e; \
	curl -fsS http://localhost:$(FACADE_PORT)/health | grep -q '"ok"' && echo "health OK"; \
	curl -fsS -XPOST http://localhost:$(FACADE_PORT)/reset >/dev/null && echo "reset OK"; \
	for amt in 100 -30 50; do \
	  curl -fsS -XPOST http://localhost:$(FACADE_PORT)/transaction \
	    -H 'content-type: application/json' \
	    -d "{\"user_id\":\"alice\",\"amount\":$$amt}" >/dev/null; \
	done; \
	echo "3 transactions queued"; \
	sleep 2; \
	balance=$$(curl -fsS http://localhost:$(FACADE_PORT)/user/alice | python3 -c 'import sys,json;print(json.load(sys.stdin)["balance"])'); \
	if [ "$$balance" = "120" ]; then echo "balance=120 OK"; else echo "FAIL: expected balance=120, got $$balance"; exit 1; fi

perf:
	uv run client/perf_test.py --base-url http://localhost:$(FACADE_PORT) --scenario 1 --clients 10 --n 1000

perf2:
	uv run client/perf_test.py --base-url http://localhost:$(FACADE_PORT) --scenario 2 --clients 10 --n 1000

minikube-start:
	minikube start

minikube-stop:
	minikube stop

clean: down
	@eval $$(minikube docker-env) && docker image prune -f || true
