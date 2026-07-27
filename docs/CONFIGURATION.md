# 설정 레퍼런스

모든 설정은 환경변수다. `Config::from_env`가 읽고 `Config::validate`가 검사하며, 검사에 걸리면
기동하지 않는다 — 잘못된 설정으로 뜨는 것보다 안 뜨는 것이 낫다는 판단이다.

이 문서가 `src/config.rs`의 knob을 하나도 빠뜨리지 않았는지는 테스트가 강제한다
(`every_configuration_knob_is_documented`). 코드에 knob을 추가하고 여기 적지 않으면 빌드가 아니라
테스트가 깨진다.

기간 값은 `500ms`, `30s`, `5m`, `2h`, `7d` 형식이다. `off`/`none`/빈 문자열은 "끔"을 뜻하며,
끌 수 있는 knob에만 유효하다.

---

## 반드시 정해야 하는 것

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_DATA_DIR` | `./data` | WAL·체크포인트·로컬 part 캐시. **이 디렉터리가 곧 미flush 데이터의 유일한 사본이다.** 장비 교체 시 이 디스크를 버리면 안 된다 |
| `LOGGYTRACY_OBJECT_STORE_URL` | 없음 (로컬 전용) | `s3://버킷/prefix` 또는 `file:///경로`. **미설정이면 S3 계층화 없이 로컬 디스크만 쓴다** — 디스크가 곧 source of truth가 되므로 운영에는 부적합 |
| `LOGGYTRACY_LISTEN_ADDR` | `0.0.0.0:3100` | Loki 호환 HTTP. TLS는 지원하지 않으므로 **신뢰 경계 안**에 두어야 한다 |
| `LOGGYTRACY_OTLP_GRPC_ADDR` | `0.0.0.0:4317` | OTLP gRPC. 트레이스와 **로그** 서비스가 같은 리스너에 붙는다. 위와 같음 |

`file://`은 **CAS를 하지 않는 단일 프로세스 개발용**이다. 공유·네트워크 스토리지에 쓰면 manifest
lost update가 나고, 그건 곧 데이터 손실이다. `from_url`이 기동 시 경고를 남긴다.

`object_store`에 넘어가는 자격증명·엔드포인트는 `AWS_*` 또는 `OBJECT_STORE_*` 환경변수로 준다
(`OBJECT_STORE_*`가 우선). S3 호환 스토어에서는 **`OBJECT_STORE_CONDITIONAL_PUT=etag`가 사실상
필수**이며, 빠지면 부팅 프리플라이트가 기동을 거부한다.

## 테넌시

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_DEFAULT_TENANT` | `default` | `X-Scope-OrgID` 없는 요청이 귀속될 테넌트 |
| `LOGGYTRACY_MISSING_TENANT_POLICY` | `default` | `default` 또는 `reject`. 헤더 없는 요청을 기본 테넌트로 받을지 거절할지 |
| `LOGGYTRACY_ALLOWED_TENANTS` | 없음 (전부 허용) | 쉼표 구분 목록. 목록 밖 테넌트는 403. **헤더는 앞단이 붙인 값을 증명 없이 신뢰하므로, 목록이 없으면 리스너에 닿는 누구나 테넌트를 만들 수 있다** |
| `LOGGYTRACY_TENANT_POLICY_TOKEN` | 없음 (기능 꺼짐) | 설정하면 테넌트별 정책 admin API가 열리고 전역 retention은 쓸 수 없게 된다 |
| `LOGGYTRACY_DEFAULT_TENANT_INGEST_BYTES_PER_SECOND` | 없음 (무제한) | control plane이 rate를 밀어주지 않은 테넌트에 적용할 기본값 |
| `LOGGYTRACY_TENANT_INGEST_BURST` | `10s` | 테넌트가 쓰지 않은 rate를 적립해 한 번에 쓸 수 있는 시간. 용량은 `MAX_PUSH_BYTES` 아래로 내려가지 않는다 |

**제약:** `MISSING_TENANT_POLICY=default`인데 `ALLOWED_TENANTS`에 기본 테넌트가 없으면 기동하지
않는다 — 헤더 없는 요청마다 목록 밖 테넌트가 생기기 때문이다.

**제약:** 저장된 테넌트 정책이 하나라도 있는데 `TENANT_POLICY_TOKEN`이 없으면 기동하지 않는다.
토큰이 없으면 쿼리 클램프가 사라져 삭제된 데이터가 되살아난다.

### 테넌트별 ingest rate는 여기 있지 않다

플랜마다 다르고 출시 후에도 바뀌므로 **control plane이 테넌트마다 push한다.** 정책 body의
`ingest_rate` 필드이며 `retention`과 같은 레코드에 산다. 값은 `4MiB/s` 같은 초당 바이트,
`0`(쓰기 금지), `unlimited` 중 하나다.

```
PUT /loggytracy/api/v1/admin/tenants/{tenant}/retention
{"retention": "7d", "ingest_rate": "4MiB/s"}
```

body는 정책 전체이지 patch가 아니다. `ingest_rate`를 빼고 push하면 기존 값이 **지워지고**
위의 기본값으로 돌아간다.

이 rate는 인스턴스 하나에 대한 몫이지 플랜이 파는 월 사용량이 아니다. 한 달치는 여러
인스턴스에 걸쳐 쓰이고 인스턴스보다 오래 사는 상태라 control plane만 들고 있을 수 있다.

## Ingest 입력 제한

전부 저널에 쓰기 **전**에 검사하므로, 거절된 요청은 WAL에 남지 않는다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_MAX_PUSH_BYTES` | 16 MiB | 압축된 push 본문 상한 |
| `LOGGYTRACY_MAX_DECOMPRESSED_PUSH_BYTES` | 64 MiB | snappy 헤더가 신고한 길이의 상한. 헤더가 할당 크기를 정하지 못하게 막는다 |
| `LOGGYTRACY_MAX_LINE_BYTES` | 256 KiB | 로그 라인 하나 |
| `LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM` | 30 | 스트림당 라벨 개수 |
| `LOGGYTRACY_MAX_LABEL_NAME_BYTES` | 1024 | |
| `LOGGYTRACY_MAX_LABEL_VALUE_BYTES` | 2048 | |
| `LOGGYTRACY_MAX_TIMESTAMP_AGE` | `7d` (`off` 가능) | 이보다 오래된 타임스탬프 거절 |
| `LOGGYTRACY_MAX_TIMESTAMP_SKEW` | `1h` (`off` 가능) | 이보다 미래인 타임스탬프 거절. **미래 part는 retention cutoff에 영원히 걸리지 않는다** — 초/밀리초를 나노초로 보내는 단위 착오가 흔하다 |

과거 데이터를 일괄 적재할 때만 두 타임스탬프 knob을 `off`로 둔다.

## Backpressure

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_MAX_MEMTABLE_BYTES` | 256 MiB (`off` 가능) | 두 memtable 합계가 넘으면 429 |
| `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` | 1 GiB (`off` 가능) | 미flush WAL이 넘으면 429 |
| `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER` | `1s` | 429에 실리는 `Retry-After` |

**제약:** `MAX_MEMTABLE_BYTES`는 `FLUSH_MAX_BYTES`보다 작을 수 없다 — flush에게 옮기라고 시키지도
않은 데이터를 이유로 쓰기를 거절하게 된다.

끄면 예전 동작(무한 증가 후 OOM)으로 돌아간다. 클라이언트가 429에 backoff하고 자체 WAL로 버티는
것이 아키텍처의 전제이므로, 끄는 것은 그 전제를 깨는 일이다.

## 저널과 flush

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_MAX_BATCH_BYTES` | 1 MiB | 한 번의 write+fsync에 묶는 최대 바이트 |
| `LOGGYTRACY_MAX_BATCH_MS` | `0` (대기 없음) | **0이 기본이자 권장.** group commit은 쓰기 뒤에서 형성된다 — write/fsync 하는 동안 도착한 것이 다음 배치가 된다. 올리면 커넥션당 처리량이 `1000/이 값` pushes/s로 묶인다. fsync가 대기보다 비싼 디스크에서만 올린다 |
| `LOGGYTRACY_FLUSH_MAX_BYTES` | 1 MiB | memtable이 이만큼 차면 flush |
| `LOGGYTRACY_FLUSH_MAX_INTERVAL` | `5s` | 크기에 못 미쳐도 이 주기로 flush. **예상치 못한 디스크 손실 시의 RPO가 이 값이다** |
| `LOGGYTRACY_FLUSH_CHECK_INTERVAL` | `500ms` | flush 루프가 조건을 확인하는 주기 |
| `LOGGYTRACY_ROW_GROUP_SIZE` | 8192 (최대 65536) | Parquet row group 행 수. 테넌트 경계에서도 끊기므로 **실제 row group 수의 하한은 part 안의 테넌트 수**다 |

## Merge

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_MERGE_INTERVAL` | `30s` | |
| `LOGGYTRACY_MERGE_MIN_PART_COUNT` | 4 (최소 2) | 이보다 적으면 일반 merge를 하지 않는다 |
| `LOGGYTRACY_MERGE_TARGET_PART_ROWS` | 1,000,000 | 출력 목표 행 수 (soft) |
| `LOGGYTRACY_MERGE_MAX_PART_ROWS` | 4,000,000 | 출력 상한 (hard) |
| `LOGGYTRACY_MERGE_MAX_INPUT_BYTES` | 512 MiB | 그룹 하나의 입력 상한. **비압축(materialized) 바이트** |
| `LOGGYTRACY_MERGE_MAX_MEMORY_BYTES` | 1 GiB | 한 번의 읽기가 materialize할 수 있는 하드 상한 |
| `LOGGYTRACY_MERGE_MAX_GROUPS_PER_TICK` | 16 | |

**제약:** `MERGE_MAX_INPUT_BYTES <= MERGE_MAX_MEMORY_BYTES`. 두 값 모두 part meta에 기록된
`materialized_bytes`(읽었을 때 실제로 차지하는 메모리)와 비교되므로 단위가 같다. 상한을 넘으면
그룹을 절반씩, 단일 part는 row group 윈도로 나눠 재작성하므로 영구 실패는 없다.

## Retention

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_RETENTION_PERIOD` | 없음 (**무한 보관**) | 전역 보관 기간. 미설정이면 S3와 디스크가 영원히 자란다 |
| `LOGGYTRACY_RETENTION_INTERVAL` | `5m` | |
| `LOGGYTRACY_RETENTION_BATCH_SIZE` | 100 | 한 tick에 처리할 part 수 |
| `LOGGYTRACY_RETENTION_GRACE_PERIOD` | `1h` | orphan 객체를 지우기 전 유예 |
| `LOGGYTRACY_MAX_RETENTION_RUNTIME` | `2m` | retention/GC 오브젝트 스토어 작업 타임아웃 |
| `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` | 0.5 | part의 만료 행 비율이 이 값을 넘으면 재작성. 테넌트 삭제(`retention: "0"`)는 이 값을 무시한다 |

**제약:** `RETENTION_PERIOD`와 `TENANT_POLICY_TOKEN`은 동시에 설정할 수 없다. 테넌트별 retention이
전역 기간을 대체하며, 조용히 하나가 무시되는 것보다 기동 실패가 낫다.

## 캐시

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_CACHE_MAX_BYTES` | 10 GiB | 로컬 part 캐시 상한. 넘으면 LRU eviction |
| `LOGGYTRACY_CACHE_EVICTION_INTERVAL` | `30s` | |

stream index 등 작은 카탈로그 파일은 eviction 대상이 아니다. 따라서 **라벨 카디널리티가 폭발하면
evict 불가능한 디스크 사용량**이 된다.

## 쿼리 리소스 상한

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_MAX_QUERY_RANGE` | 없음 | 요청 가능한 최대 시간 범위 |
| `LOGGYTRACY_MAX_QUERY_SCAN_ROWS` | 5,000,000 | |
| `LOGGYTRACY_MAX_QUERY_SCAN_BYTES` | 2 GiB | |
| `LOGGYTRACY_MAX_QUERY_MEMORY_BYTES` | 512 MiB | |
| `LOGGYTRACY_MAX_LOG_LIMIT` | 100,000 | `limit` 파라미터 상한 |
| `LOGGYTRACY_MAX_QUERY_RUNTIME` | `30s` | 메타데이터 엔드포인트의 타임아웃이기도 하다 |
| `LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS` | 8 | 메타데이터 엔드포인트와 공유한다 |
| `LOGGYTRACY_MAX_SERIES_MATCHERS` | 32 | `series`의 `match[]` 개수. 매처 하나가 전체 패스 하나다 |
| `LOGGYTRACY_MAX_RESTORE_RUNTIME` | `25s` | 캐시 미스 복원 타임아웃 |

### 메트릭 쿼리

| 변수 | 기본값 |
|---|---|
| `LOGGYTRACY_MAX_METRIC_EVALUATION_POINTS` | 10,000 |
| `LOGGYTRACY_MAX_METRIC_ROWS` | 1,000,000 |
| `LOGGYTRACY_MAX_METRIC_SERIES` | 100,000 |
| `LOGGYTRACY_MAX_METRIC_SAMPLES` | (`config.rs` 참조) |
| `LOGGYTRACY_MAX_CONCURRENT_METRIC_EVALUATIONS` | 4 |

### 트레이스 쿼리

| 변수 | 기본값 |
|---|---|
| `LOGGYTRACY_MAX_TRACE_SPANS` | 100,000 |
| `LOGGYTRACY_MAX_TRACE_SEARCH_LIMIT` | 1,000 |
| `LOGGYTRACY_MAX_CONCURRENT_TRACE_SCANS` | 8 |
| `LOGGYTRACY_MAX_TRACE_QUERY_RUNTIME` | `30s` |
| `LOGGYTRACY_MAX_TRACE_RESTORE_RUNTIME` | `25s` |

## 기동과 종료

| 변수 | 기본값 | 설명 |
|---|---|---|
| `LOGGYTRACY_STARTUP_RETRY_BUDGET` | `5m` | 오브젝트 스토어 기동 단계를 이 시간까지 재시도한다. 일시 장애를 흡수하되, 넘기면 종료해 오케스트레이터의 재시작 backoff에 넘긴다 |
| `LOGGYTRACY_SHUTDOWN_FLUSH_WARN_AFTER` | `30s` | force-flush가 이만큼 실패하면 stdout에 운영자 경고 |

## 부하 하네스 전용 (운영에서 쓰지 않는다)

`scripts/run_load_local.sh`가 쓰는 인프로세스 지연·오류 주입이다. 하나라도 설정되면 래퍼가
활성화되므로, **운영 환경에는 절대 두지 않는다.**

| 변수 | 설명 |
|---|---|
| `LOGGYTRACY_OBJECT_STORE_LATENCY_MS` | 쓰기 지연 base |
| `LOGGYTRACY_OBJECT_STORE_READ_LATENCY_MS` | 읽기 지연 base (미설정 시 쓰기 값) |
| `LOGGYTRACY_OBJECT_STORE_LATENCY_JITTER_MS` | 위에 더해지는 `uniform(0, jitter)` |
| `LOGGYTRACY_OBJECT_STORE_ERROR_RATE` | 0.0~1.0. **쓰기에만** 주입된다 |
| `LOGGYTRACY_OBJECT_STORE_FAULT_SEED` | 재현용 시드 |

## 시계

프로덕션에서 설정할 것은 없다. 다만 시간 의존 동작이 어떻게 검사되는지는 알아 둘 값어치가 있다.

- **단조 시계**(flush 주기, force-flush backoff, 기동 재시도 예산)는 `tokio::time::Instant`를 쓴다.
  `tokio::time::pause()`가 이것을 가상화하므로, 5분짜리 예산을 10밀리초에 검사한다.
- **벽시계**(타임스탬프 수용 윈도우, 쿼리 기본 범위, retention cutoff)는 `Clock`을 통해 읽는다.
  테스트가 시계를 세우고 밀 수 있어서 경계를 정확히 겨냥할 수 있다 — 데이터를 과거로 조작하는
  대신 시간을 움직인다.

## 로깅

`RUST_LOG`를 그대로 따른다. 미설정 시 `loggytracy=info,warn`.

---

## 튜닝의 출발점

- **RPO를 줄이고 싶다** → `FLUSH_MAX_INTERVAL`을 낮춘다. 오브젝트 스토어 쓰기 횟수가 그만큼 는다
- **ack 지연이 높다** → `MAX_BATCH_MS`가 0인지 먼저 본다. 0이 아니면 그 값이 곧 지연의 하한이다
- **WAL backlog가 는다** → flush가 ingest를 못 따라가는 것이다. 429가 나오는지 보고
  (`loggytracy_ingest_throttled_total`), 안 나오면 상한이 너무 높은 것이다
- **`/ready`가 503에서 안 돌아온다** → `/metrics`의 `*_errors_total` 중 무엇이 늘고 있는지 본다.
  flush·merge·retention·오브젝트 스토어·로컬 캐시가 각각 독립적으로 readiness를 내린다
- **디스크가 찬다** → `CACHE_MAX_BYTES`를 줄이거나 `RETENTION_PERIOD`를 설정한다. 후자가
  미설정이면 아무것도 지워지지 않는다
- **p95/p99를 보고 싶다** → `loggytracy_query_latency_ms_bucket`에 `histogram_quantile`을 쓴다.
  `*_latency_ns_total`은 평균만 준다
