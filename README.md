# spark-auto-remediation

실패한 Spark 적재 잡의 원인을 분류하고, 원인에 맞는 설정 하나를 고쳐 복구한다.

소스(파일, Postgres)에서 읽어 Iceberg에 적재하는 Spark 잡이 실패했을 때

1. 로그·지표·Pod 종료 사유로 원인을 분류한다
2. 리소스 문제면 원인에 맞는 레버 하나를 한 단계 조정하고, 리소스 문제가 아니면 재시도하지 않는다
3. 성공한 설정을 해당 잡의 다음 기본값으로 쓴다

증명할 것: **"실패하면 메모리 2배"보다 적은 자원으로 더 많은 실패를 복구하고, 메모리로 해결되지 않는 실패를 구분한다.**

## 구조

```
infra/          k3d 클러스터, MinIO·Postgres·RBAC·History Server, ops 스키마
images/spark/   Spark 4.1.4 + Iceberg 1.12.0 + aws-bundle + PostgreSQL JDBC
config/         잡별 설정 (기본 리소스, 공통 conf)
jobs/           적재 잡과 실패 재현 잡 (PySpark)
remediator/     제출·수집·분류
```

## 환경

k3d (server 1 + agent 2, agent당 3.5GiB) · MinIO · Postgres · kubeflow spark-operator · Spark History Server

```bash
make up                        # 클러스터 + 기반 서비스 + ops DB
make job JOB=udf_oom_killed    # 잡 제출
make status
```

## 실패를 읽는 경로

잡 안에는 수집 코드가 없다. 신호를 전부 클러스터 밖에서 읽는다.

| 경로 | 얻는 것 |
|---|---|
| `kubectl get sparkapplication` | 최종 상태, `errorMessage` |
| `kubectl logs <driver-pod>` | driver 예외, executor 실패 전파, 태스크 스택트레이스 |
| `kubectl get pod <exec-pod>` | `terminated.reason` / `exitCode` — 137(컨테이너 OOM)과 52(JVM heap OOM) 구분 |
| History Server REST | 실패 stage, 태스크 수, spill, executor 피크 지표 |

OOMKilled는 Pod status에만 남고 event log에는 없다. 그래서 두 경로를 모두 읽는다.

## 분류와 대처

Postgres `ops` DB에 둔다. 오류 유형과 대처 규칙은 사전 데이터로 들어가 있고, 실패가 쌓이면서 고친다.

```
error_types          대표 오류 8개 (heap, jvm_overhead, python_mem, skew_partition,
                     jdbc_partition, driver, non_resource, unknown)
remediation_rules    오류별 대처 — 레버 하나, 이산 단계 (예: python_mem → Arrow 배치↓)
failures             실패 1건 + 분류 + 제안
failure_executors    executor별 종료 사유·피크
failure_stages       실패 stage별 모양
runs                 돌린 실행과 결과
```

Iceberg는 잡이 적재하는 대상으로만 쓴다. 스냅샷 summary의 `run_id`로 "커밋 후 실패"를 판정한다.

## 진행 상황

| 단계 | 상태 |
|---|---|
| 환경 (k3d, MinIO, Postgres, operator, History Server) | 완료 |
| Spark 이미지, 소스 데이터 (TPC-DS + 합성) | 완료 |
| `ops` 테이블과 사전 데이터 | 완료 |
| 제출 (`remediator/submit.py`) | 완료 |
| 실패 재현 잡 | 1개 (`udf_oom_killed`) |
| 수집·분류·적재 | 미구현 |
| 조정과 재제출 | 미구현 |
| 전략 비교 리포트 | 미구현 |

재제출은 아직 사람이 한다 (`make job JOB=... SET=path=value`).
자동 조정은 `remediation_rules`가 쌓인 실패와 맞아떨어지는지 확인한 뒤 붙인다.
