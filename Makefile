SHELL := /bin/bash
CLUSTER := spark-ar
CTX := k3d-$(CLUSTER)
DATA := $(CURDIR)/.data
KUBECTL := kubectl --context $(CTX)

# s3a 용 jar (apache/spark 이미지에 없음). 호스트 ./.data/jars 에 두고 모든 노드가 공유한다.
HADOOP_AWS_URL := https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
AWS_BUNDLE_URL := https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

.PHONY: help up down cluster jars secrets infra ops-db operator image venv job smoke status

help:
	@grep -E '^[a-z0-9-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

up: jars cluster secrets infra ops-db operator image ## 클러스터 + 기반 서비스 전체 기동

jars: ## s3a jar 내려받기 (이미 있으면 건너뜀)
	@mkdir -p $(DATA)/jars
	@for u in $(HADOOP_AWS_URL) $(AWS_BUNDLE_URL); do \
	  f=$(DATA)/jars/$$(basename $$u); \
	  [ -s $$f ] || { echo "downloading $$(basename $$u)"; curl -fsSL -o $$f $$u; }; \
	done
	@ls -lh $(DATA)/jars

cluster: ## k3d 클러스터 생성 (server1 + agent2, agent 당 3.5GiB)
	@k3d cluster list $(CLUSTER) >/dev/null 2>&1 \
	  && echo "cluster $(CLUSTER) already exists" \
	  || k3d cluster create --config infra/k3d/cluster.yaml -v "$(DATA):/var/lib/rancher/k3s/storage@all"

secrets: ## .env 값으로 k8s Secret 생성
	@test -f .env || { echo ".env 가 없다. cp .env.example .env"; exit 1; }
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

infra: ## MinIO / Postgres / 버킷 / RBAC / History Server 배포
	$(KUBECTL) apply -f infra/manifests/
	$(KUBECTL) -n storage rollout status deploy/minio --timeout=180s
	$(KUBECTL) -n storage rollout status deploy/postgres --timeout=180s
	$(KUBECTL) -n storage wait --for=condition=complete job/minio-buckets --timeout=180s
	$(KUBECTL) -n spark rollout status deploy/spark-history-server --timeout=180s

ops-db: ## ops DB 생성 + 실패 관리 스키마·사전 데이터 적재 (멱등)
	@set -a; source .env; set +a; \
	pod=$$($(KUBECTL) -n storage get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}'); \
	psql="$(KUBECTL) -n storage exec -i $$pod -- psql -U $$POSTGRES_USER"; \
	$$psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='ops'" | grep -q 1 \
	  || $$psql -d postgres -c "CREATE DATABASE ops"; \
	$$psql -d ops -tAc "SELECT to_regclass('public.error_types') IS NOT NULL" | grep -q t \
	  || { $$psql -d ops -v ON_ERROR_STOP=1 -f - < infra/sql/01-ops-schema.sql; \
	       $$psql -d ops -v ON_ERROR_STOP=1 -f - < infra/sql/02-ops-seed.sql; }; \
	$$psql -d ops -c "SELECT (SELECT count(*) FROM error_types) AS 오류유형, \
	  (SELECT count(*) FROM remediation_rules) AS 대처규칙, \
	  (SELECT count(*) FROM failures) AS 실패, (SELECT count(*) FROM runs) AS 실행"

operator: ## kubeflow spark-operator 설치 (chart 2.5.2)
	helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
	helm repo update spark-operator >/dev/null
	helm upgrade --install spark-operator spark-operator/spark-operator \
	  --version 2.5.2 \
	  --namespace spark-operator --create-namespace \
	  --kube-context $(CTX) \
	  -f infra/helm/spark-operator-values.yaml \
	  --wait --timeout 5m

IMAGE := spark-ar:4.1.4-iceberg1.12.0

image: ## Spark 이미지 빌드 + k3d 노드에 로드
	docker build -t $(IMAGE) -f images/spark/Dockerfile images/spark
	k3d image import $(IMAGE) -c $(CLUSTER)

venv: ## 호스트 파이썬 환경
	test -d .venv || python3 -m venv .venv
	.venv/bin/pip install -q --disable-pip-version-check "pyyaml==6.0.3"

job: ## 잡 하나 제출: make job JOB=udf_oom_killed [SET=path=value]
	@test -n "$(JOB)" || { echo "JOB= 를 지정해라. 예: make job JOB=udf_oom_killed"; exit 1; }
	.venv/bin/python remediator/submit.py $(JOB) $(if $(SET),--set $(SET),)

smoke: ## SparkPi 제출 → event log → History Server REST 확인 (Phase 1 검증)
	-$(KUBECTL) -n spark delete sparkapplication spark-pi-smoke --ignore-not-found
	$(KUBECTL) apply -f infra/smoke/sparkpi.yaml
	@for i in $$(seq 1 60); do \
	  s=$$($(KUBECTL) -n spark get sparkapplication spark-pi-smoke -o jsonpath='{.status.applicationState.state}' 2>/dev/null); \
	  echo "  state=$$s"; \
	  [ "$$s" = "COMPLETED" ] && break; \
	  [ "$$s" = "FAILED" ] && { $(KUBECTL) -n spark logs spark-pi-smoke-driver --tail=50; exit 1; }; \
	  sleep 10; \
	done
	curl -s http://localhost:18080/api/v1/applications | head -c 2000

status: ## 클러스터/서비스 상태
	$(KUBECTL) get nodes -o wide
	$(KUBECTL) get pods -A -o wide

down: ## 클러스터 삭제 (./.data 는 남는다)
	k3d cluster delete $(CLUSTER)
