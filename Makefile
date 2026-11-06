SHELL := /bin/bash
CLUSTER := spark-ar
CTX := k3d-$(CLUSTER)
DATA := $(CURDIR)/.data
KUBECTL := kubectl --context $(CTX)

# Jars for s3a, absent from the apache/spark image. Kept in ./.data/jars and shared by all nodes.
HADOOP_AWS_URL := https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
AWS_BUNDLE_URL := https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

.PHONY: help up down cluster jars secrets infra ops-db operator image data job classify reclassify failures status \
        airflow-up airflow-down airflow-status

help:
	@grep -E '^[a-z0-9-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

up: jars cluster secrets infra ops-db operator image ## bring up the cluster and all base services

jars: ## download the s3a jars (skipped if present)
	@mkdir -p $(DATA)/jars
	@for u in $(HADOOP_AWS_URL) $(AWS_BUNDLE_URL); do \
	  f=$(DATA)/jars/$$(basename $$u); \
	  [ -s $$f ] || { echo "downloading $$(basename $$u)"; curl -fsSL -o $$f $$u; }; \
	done
	@ls -lh $(DATA)/jars

cluster: ## create the k3d cluster (1 server + 2 agents, 3.5GiB each)
	@k3d cluster list $(CLUSTER) >/dev/null 2>&1 \
	  && echo "cluster $(CLUSTER) already exists" \
	  || k3d cluster create --config infra/k3d/cluster.yaml -v "$(DATA):/var/lib/rancher/k3s/storage@all"

secrets: ## create the k8s secrets from .env
	@test -f .env || { echo ".env is missing. cp .env.example .env"; exit 1; }
	@set -a; source .env; set +a; \
	$(KUBECTL) apply -f infra/manifests/00-namespaces.yaml; \
	for ns in storage spark; do \
	  $(KUBECTL) -n $$ns create secret generic minio-credentials \
	    --from-literal=MINIO_ROOT_USER="$$MINIO_ROOT_USER" \
	    --from-literal=MINIO_ROOT_PASSWORD="$$MINIO_ROOT_PASSWORD" \
	    --dry-run=client -o yaml | $(KUBECTL) apply -f -; \
	  $(KUBECTL) -n $$ns create secret generic postgres-credentials \
	    --from-literal=POSTGRES_USER="$$POSTGRES_USER" \
	    --from-literal=POSTGRES_PASSWORD="$$POSTGRES_PASSWORD" \
	    --dry-run=client -o yaml | $(KUBECTL) apply -f -; \
	done

infra: ## deploy MinIO, Postgres, buckets, RBAC and the History Server
	$(KUBECTL) apply -f infra/manifests/
	$(KUBECTL) -n storage rollout status deploy/minio --timeout=180s
	$(KUBECTL) -n storage rollout status deploy/postgres --timeout=180s
	$(KUBECTL) -n storage wait --for=condition=complete job/minio-buckets --timeout=180s
	$(KUBECTL) -n spark rollout status deploy/spark-history-server --timeout=180s

ops-db: ## create the ops database and load the schema and seed data (idempotent)
	@set -a; source .env; set +a; \
	pod=$$($(KUBECTL) -n storage get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}'); \
	psql="$(KUBECTL) -n storage exec -i $$pod -- psql -U $$POSTGRES_USER"; \
	$$psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='ops'" | grep -q 1 \
	  || $$psql -d postgres -c "CREATE DATABASE ops"; \
	$$psql -d ops -tAc "SELECT to_regclass('public.error_types') IS NOT NULL" | grep -q t \
	  || $$psql -d ops -v ON_ERROR_STOP=1 -f - < infra/sql/01-ops-schema.sql; \
	$$psql -d ops -v ON_ERROR_STOP=1 -f - < infra/sql/02-ops-seed.sql > /dev/null; \
	$$psql -d ops -c "SELECT (SELECT count(*) FROM error_types) AS error_types, \
	  (SELECT count(*) FROM remediation_rules) AS rules, \
	  (SELECT count(*) FROM failures) AS failures, (SELECT count(*) FROM runs) AS runs"

operator: ## install the kubeflow spark-operator (chart 2.5.2)
	helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
	helm repo update spark-operator >/dev/null
	helm upgrade --install spark-operator spark-operator/spark-operator \
	  --version 2.5.2 \
	  --namespace spark-operator --create-namespace \
	  --kube-context $(CTX) \
	  -f infra/helm/spark-operator-values.yaml \
	  --wait --timeout 5m

IMAGE := spark-ar:4.1.4
SF := 1                  # TPC-DS scale factor

image: ## build the Spark image and load it into the k3d nodes
	docker build -t $(IMAGE) -f images/spark/Dockerfile images/spark
	k3d image import $(IMAGE) -c $(CLUSTER)

data: ## generate source data: TPC-DS (SF=$(SF)) and synthetic, into MinIO landing and Postgres source
	.venv/bin/python data/gen_tpcds.py --sf $(SF)
	.venv/bin/python data/gen_synthetic.py

job: ## submit one job: make job JOB=udf_oom_killed [SET=path=value]
	@test -n "$(JOB)" || { echo "JOB= is required, e.g. make job JOB=udf_oom_killed"; exit 1; }
	.venv/bin/python faults/submit.py $(JOB) $(if $(SET),--set $(SET),)

classify: ## collect, classify and store failures: make classify [APP=<app-name>], else every FAILED
	.venv/bin/python remediator/classifier.py $(if $(APP),$(APP),--all)

reclassify: ## re-classify stored raw and print differences (regression check). APPLY=1 writes them back
	.venv/bin/python remediator/classifier.py --reclassify $(if $(APPLY),--apply,)

failures: ## list stored failures and their suggestions
	@set -a; source .env; set +a; \
	pod=$$($(KUBECTL) -n storage get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}'); \
	$(KUBECTL) -n storage exec -i $$pod -- psql -U $$POSTGRES_USER -d ops -c \
	  "SELECT failure_id id, job_name, error_code, suggest_path, suggest_from AS from_, \
	          suggest_to AS to_, coalesce(no_suggest_reason,'') AS note \
	   FROM failures ORDER BY failure_id DESC LIMIT 20" -c \
	  "SELECT error_code, coalesce(failure_mode,'-') AS failure_mode, count(*) \
	   FROM failures GROUP BY 1,2 ORDER BY 1,2"

airflow-up: ## start Airflow on the host (metadata DB container plus the four processes)
	./infra/airflow/start.sh

airflow-down: ## stop Airflow (the metadata DB container keeps running)
	./infra/airflow/stop.sh

airflow-status: ## Airflow processes and API status
	@. infra/airflow/env.sh; \
	for n in api-server scheduler dag-processor triggerer; do \
	  p=$$(cat "$$AIRFLOW_HOME/run/$$n.pid" 2>/dev/null); \
	  if kill -0 "$$p" 2>/dev/null; then echo "  $$n  pid $$p"; \
	  else echo "  $$n  down"; fi; \
	done; \
	echo "  metadata DB  $$(docker ps --filter name=airflow-postgres --format '{{.Status}}' || echo down)"; \
	echo "  API     $$(curl -s http://localhost:$$AIRFLOW_API_PORT/api/v2/version || echo "no response")"

status: ## cluster and service status (finished Spark job pods excluded)
	@$(KUBECTL) get nodes
	@$(KUBECTL) get pods -A --field-selector=status.phase!=Succeeded,status.phase!=Failed

down: ## delete the cluster (./.data is kept)
	k3d cluster delete $(CLUSTER)
