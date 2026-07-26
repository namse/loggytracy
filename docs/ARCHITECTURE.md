# loggytracy 아키텍처

Rust로 만드는 싱글 머신 log + trace 엔진. VictoriaLogs의 논리 설계에 Parquet 물리 포맷과 S3 계층화를 결합한다.

## 확정된 결정

| 항목 | 결정 |
|---|---|
| 배포 형태 | 싱글 머신, 단일 writer |
| Source of truth | S3 호환 오브젝트 스토리지 |
| 로컬 디스크 | 캐시 (LRU eviction) |
| 내구성 | 저널(append-only) + group commit + fsync 후 ack. Alloy WAL을 안전망으로 전제 |
| 복제 | 레플리카 없음. 예상치 못한 서버/디스크 손실 시 손실 허용 윈도우(RPO)는 flush 주기(`flush_max_bytes`/`flush_max_interval`, 기본 1MiB/5초 중 먼저 도달하는 쪽)로 결정되며 이를 의도적으로 수용한다 |

## 내구성/복구 시맨스

- **WAL + checkpoint 불변량**: ack된 레코드는 항상 WAL에 있고 동시에 memtable에 insert된다(insert는 writer 태스크가 write 성공 후 ack 직전에 수행). 따라서 `checkpoint()`가 캡처하는 (offset, memtable snapshot)은 원자적으로 일치한다.
- **복구**: 시작 시 WAL의 `[checkpoint..replay_end]`만 replay하여 memtable에 적재하고, corrupt/partial tail은 `replay_end`로 truncate한다. checkpoint는 복구 단계에서 전진시키지 않는다 — in-flight 데이터는 아직 memtable에만 있으므로, 다음 flush의 `checkpoint()`가 올바른 offset을 잡을 때까지 checkpoint를 그대로 둔다. 따라서 "재시작 → 재시작"을 반복해도 in-flight 데이터가 유실되지 않는다.
- **at-least-once (flush 경계)**: flush가 part 디스크 writes 완료 후 `set_checkpoint` 직전에 크래시하면, 해당 part와 다음 replay가 같은 데이터를 모두 포함하게 되어 중복이 발생할 수 있다. 이는 정확성보다 내구성 우선의 의도적 트레이드오프이며, 중복은 쿼리 결과에 나타날 수 있고 중복 제거는 후속 마일스톤에서 다룬다.
- **flush 가시성 경계**: 쿼리는 part/memtable operation read lock을 전체 스캔 동안 유지하고, flush는 part 등록과 flushing 버퍼 commit을 같은 operation write lock 안에서 수행한다. 따라서 정상 flush와 겹친 metric/log query가 같은 row를 memtable과 part에서 동시에 세지 않는다. 다만 flush가 part commit 후 checkpoint 전에 중단되는 at-least-once 복구 중복은 여전히 가능하며, 영속 deduplication은 후속 마일스톤이다.
- **merge 교체 불변량**: merge tombstone은 새 part 디렉터리가 `.tmp`에서 최종 위치로 rename되기 전에 기록된다. 재시작 복구는 새 part를 성공적으로 open한 경우에만 old part를 삭제하며, 새 part 검증에 실패하면 old part를 유지한다.
- **merge tombstone 연쇄 복구**: 재시작 시 모든 tombstone 관계를 먼저 수집하고 이전 세대까지 폐쇄적으로 추적한 뒤 old part를 정리한다. 따라서 삭제 실패 후 여러 세대의 merge가 겹쳐도 중간 tombstone 삭제로 이전 세대 part가 부활하지 않는다.
- **WAL compaction**: object store가 활성화된 경우 part 업로드와 manifest CAS가 성공한 뒤 writer 태스크가 checkpoint 이전 WAL 구간을 제거한다. 교체 전에 checkpoint를 0으로 되돌리므로 compaction 도중 크래시는 이전 WAL 전체 또는 새 WAL suffix를 재생하며, 중복은 가능하지만 유실은 없다. 로컬 전용 모드는 기존 offset checkpoint를 유지한다.
- **예상치 못한 디스크 손실**: 로컬 디스크가 통째로 소실되면, 마지막으로 성공한 flush(S3 업로드 + manifest 갱신) 이후 아직 flush되지 않은 WAL/MemTable 데이터는 서버 쪽에서 복구할 수 없다. 이 손실 허용 윈도우는 `flush_max_bytes`/`flush_max_interval`(기본 1MiB/5초, 둘 중 먼저 도달하는 쪽)로 결정되며, 레플리카 없이 이 정도 손실은 의도적으로 수용한다. 계획된 장비 교체는 이 윈도우가 적용되지 않도록 아래 graceful shutdown 절차를 따른다.

| 물리 포맷 | Parquet (dictionary + zstd) + 사이드카 인덱스 파일 |
| 인덱스 | stream index + 블록별 trigram bloom filter (역인덱스 없음) |
| 쿼리 언어 | LogQL — 사용 빈도 높은 subset만, 미지원 문법은 명확한 에러 |
| API | Loki HTTP API 호환 (Grafana Loki 데이터소스 직결), 트레이스는 Tempo API |
| Ingest 프로토콜 | Loki push (protobuf+snappy) + OTLP (gRPC) |
| 전송 보안 | **TLS를 지원하지 않는다.** 평문 HTTP/gRPC만 제공하며, 종단 암호화가 필요하면 리버스 프록시나 서비스 메시가 담당한다 |
| 테넌시 | 멀티테넌트. `X-Scope-OrgID`로 테넌트를 구분하고, 테넌트가 스로틀·quota의 단위가 된다 |
| 검증 환경 | **실 S3에 붙여 검증하지 않는다.** 부하 검증의 상한은 로컬 MinIO이며, 남은 위험은 첫 실 배포 절차로 관리한다 |

## 전송 보안 — TLS 미지원

TLS 종단은 이 프로세스의 책임이 아니다. 인증서 발급·갱신·SNI·mTLS 정책은 이미 잘 하는 계층
(리버스 프록시, ingress, 서비스 메시)에 맡기고, 엔진은 평문 HTTP와 평문 gRPC만 제공한다.
저장 계층의 S3 접근은 `object_store`가 HTTPS를 쓰므로 이 결정과 무관하다.

따라서 배포 요구사항은 다음과 같다.

- 리스닝 주소는 신뢰 경계 안에 두어야 한다. 공개망에 직접 노출하는 구성은 지원하지 않는다.
- 인증·인가도 이 프로세스 밖(프록시 또는 게이트웨이)에서 수행하되, 프록시가 검증한 테넌트를
  `X-Scope-OrgID`로 전달해야 한다. 엔진은 이 헤더를 신뢰한다 — 즉 **헤더를 위조할 수 있는
  네트워크 위치에서 엔진에 직접 접근 가능하면 테넌트 격리가 무너진다.**
- 로컬 개발 외에는 `0.0.0.0` 바인드를 쓰지 않는 것을 권한다.

## 검증 환경 — 실 S3 미검증

인디 프로젝트이므로 실 클라우드 계정에서 의미 있는 기간·규모의 부하를 걸지 않는다. 부하 검증의
상한은 로컬 MinIO(Tier C)와 인프로세스 지연·장애 주입(Tier B)이다.

이 결정이 무엇을 잃는지는 분명히 해 둘 필요가 있다. MinIO는 `AmazonS3` 코드 경로를 그대로 태우므로
**프로토콜과 manifest CAS는 진짜로 검증된다.** 반면 다음은 검증되지 않으며, 없어진 위험이 아니라
남은 위험이다.

- **지연 분포의 꼬리** — 루프백은 1 ms 미만이고 실 S3는 p99가 수백 ms~수 초다. Tier B의 지연
  주입이 이 축을 대신하지만, 주입값은 실측이 아니라 가정이다.
- **스로틀링** — S3는 prefix당 요청률을 제한하고 MinIO는 하지 않는다. 이 엔진의 manifest는
  단일 키를 flush마다 재작성하는 hot-key 패턴이라 특히 관련이 있다. 완화 수단이 없다.
- **비용** — 이 설계는 R2 Class A 비용에 지배당해 왔는데 MinIO는 비용 신호를 주지 않는다.
  금액은 못 재도 연산 횟수는 로컬에서 잴 수 있다.
- **제공자별 조건부 PUT 시맨스** — MinIO에서 CAS가 동작한다는 것이 R2에서 동작한다는 뜻은 아니다.

따라서 배포 요구사항이 하나 늘어난다. **배포 대상 스토어에서 CAS가 동작하는지는 반드시 직접
확인해야 한다.** 부하가 아니라 기능 확인이므로 몇 분이면 되고, CAS 없이 운영하면 manifest
lost update가 곧 데이터 손실이다.

절차와 수용 기준은 [`LOAD_VALIDATION.md`](LOAD_VALIDATION.md)에 있다.

## 테넌시

테넌시는 부가 기능이 아니라 **자원 관리의 기본 단위**다. 스로틀과 quota를 테넌트별로 운영할
것이므로, 테넌트는 ingest·저장·쿼리 전 경로에서 1급 식별자여야 한다.

- **식별**: `X-Scope-OrgID` 헤더 (Loki/Tempo 관례와 동일). OTLP는 gRPC 메타데이터의 동명 키를 쓴다.
  값은 journal append **이전에** `[a-zA-Z0-9_-]{1,64}` 허용 목록으로 검증한다 — 이 값이 그대로
  object store 키와 로컬 파일 경로에 들어가기 때문이다.
  - `LOGGYTRACY_DEFAULT_TENANT` (기본 `default`): 헤더 없는 요청에 적용할 테넌트.
  - `LOGGYTRACY_MISSING_TENANT_POLICY` (기본 `default`): `default`면 위 테넌트로 수용하고,
    `reject`면 400으로 거절한다. 헤더가 **빈 값**인 경우는 두 정책 모두 거절한다 —
    클라이언트 버그를 다른 테넌트로 조용히 흘려보내지 않기 위해서다.
- **격리 지점**: 테넌트는 저장 경로의 분할 축이 **아니라** part 내부의 정렬 키이자 인덱스 축이다.
  하나의 part object가 모든 테넌트를 담고, 행은 `(tenant, timestamp_ns)`로 정렬되며 row group은
  테넌트 경계를 넘지 않는다. `meta.json`의 테넌트 인덱스가 각 테넌트의 row group 구간과
  min/max 타임스탬프를 담고, 모든 읽기 경로는 이 구간 밖의 row group을 **주소로 지정할 수 없다**.
  테넌트를 경로 축으로 두는 예전 설계는 R2 Class A 비용 때문에 폐기됐다 —
  자세한 비용 모델과 근거는 [`docs/MULTI_TENANCY_DESIGN.md`](MULTI_TENANCY_DESIGN.md).
- **retention**: 플랜 속성이므로 테넌트마다 다르다. control plane이 테넌트 하나씩 push하고,
  loggytracy는 오브젝트 스토어에 저장한 뒤에야 성공 응답을 준다. 적용은 **삭제 시점**이다 —
  쓰기 시점이 아니므로 플랜 업그레이드·다운그레이드가 이미 기록된 데이터에도 그대로 반영된다.
  push된 적 없는 테넌트는 **정책 미상이며 미상은 보존**이다. 테넌트 삭제는 retention `0`이다.
  자세한 내용은 [`docs/RETENTION_DESIGN.md`](RETENTION_DESIGN.md).
- **스로틀·quota 대상**: ingest rate(bytes/s, events/s), 활성 스트림 수(카디널리티),
  저장 용량, 동시 쿼리 수, 쿼리 스캔 예산. 초과 시 ingest는 `429`(Alloy가 backoff하고 자체 WAL로
  버티게 한다), 쿼리는 `429` 또는 `422`로 응답한다.
- **관측**: 모든 quota 카운터와 거절 카운터는 테넌트 라벨을 붙여 `/metrics`에 노출해야 한다.
  quota를 운영하려면 "누가 어디서 얼마나 막혔는지"가 보여야 한다.
- **현재 상태**: 식별·검증·격리는 구현됐다. `X-Scope-OrgID`는 Loki push와 OTLP gRPC에서 추출되어
  WAL 레코드에 함께 기록되고(재시작 후에도 소유자가 유지된다), MemTable·part·trace part·쿼리·
  카탈로그 조회는 모두 테넌트를 필수 인자로 받는다. `/metrics`만 운영자용 프로세스 전역 집계를
  유지한다. 아직 없는 것은 **테넌트별 quota·스로틀·durable 사용량 회계**와 tier 파티셔닝이며,
  이는 `docs/MULTI_TENANCY_DESIGN.md`의 5·6단계다
  (`docs/PRODUCTION_READINESS_REVIEW.md` P0-3 참고).

## 데이터 모델

- 로그와 스팬 모두 "timestamp + 필드 집합"인 wide event로 통일 (OTel 데이터 모델 기준).
- 필드는 두 계층:
  - **stream fields**: 저카디널리티 (`app`, `host` 등). 스트림 식별자이며 stream index에 인덱싱. LogQL의 라벨에 대응.
  - **일반 필드**: 고카디널리티 허용 (`user_id`, `trace_id` 등). 컬럼으로 저장하고 bloom filter로 프루닝. LogQL 파이프라인 필터(`| user_id="123"`)에 대응.
- ingest 시 값 타입 감지 (숫자, timestamp, IP 등) 후 문자열이 아닌 타입 컬럼으로 저장.

## 쓰기 경로

```
Alloy ──▶ Ingest API
            │
            ▼
         저널 append (순차 쓰기, group commit: N MB 또는 T ms 중 먼저)
            │ fsync 완료 후 일괄 ack
            ▼
         MemTable (Arrow RecordBatch, 즉시 쿼리 가능)
            │ 크기/시간 기준 flush (ack과 무관하게 느긋하게)
            ▼
         Part 생성 (immutable): Parquet + 사이드카
            │
            ▼
         S3 업로드 → manifest 갱신 → 저널 truncate
```

- 크래시 복구 = 저널 재생. part 크기는 ingest 속도와 무관하게 결정.
- 백그라운드 merge: 작은 part → 큰 part (LSM 스타일). 일 단위 시간 파티션.
- out-of-order 타임스탬프는 merge 시 정렬로 흡수. 파티션 경계를 벗어나는 지연 데이터의 허용 윈도우는 설정으로.

## Part 구조

part는 immutable 디렉터리 하나:

- `data.parquet` — 해당 part에 실제 존재하는 필드로 스키마 구성 (part별 동적 스키마). 희귀 필드는 map 컬럼으로.
- trigram bloom filter 사이드카 — row group 단위. `_msg` 등 텍스트 컬럼의 3-gram. 부분문자열 검색(`|=`) 프루닝용. 단어 쿼리도 trigram으로 커버됨.
- stream index 사이드카 — stream fields → row group posting (roaring bitmap).
- 메타 파일 — 시간 범위, row 수, 필드 목록, min/max와 part 파일별 CRC32. 메타 자체도 CRC32로 검증하며, 불일치한 part는 로드하지 않는다.

bloom은 프루닝 전용이며 최종 판정은 항상 블록 스캔이므로 정확성은 스캔이 보장한다.

## 읽기 경로

LogQL 파싱(chumsky) → 플랜 → 프루닝 단계 순서:

1. 시간 범위 → 파티션/part 선택 (manifest + part 메타)
2. 라벨 매처 → stream index로 row group 프루닝
3. line filter(`|=`, `|~`) 및 구조화 필드 필터 → trigram bloom으로 row group 프루닝
4. 남은 row group만 스캔 (MemTable + 로컬 part + S3 range read)

- Loki에서 느린 `| json | field="x"` 패턴을, ingest 시 컬럼화된 필드라면 bloom 프루닝으로 push-down하여 가속하는 것이 이 엔진의 차별점. 플래너에 push-down을 처음부터 설계에 포함.
- 실행 엔진으로 DataFusion 채택 검토 (custom TableProvider로 프루닝 결과를 공급). LogQL 특유의 range aggregation은 커스텀 연산자.

## S3와 manifest

- part 업로드 완료 후 manifest 갱신. manifest는 버전 번호가 있는 파일이며 S3 conditional write(If-None-Match)로 compare-and-swap.
- 로컬 디스크는 part 캐시. 작은 metadata/bloom/stream index catalog는 유지하고, `data.parquet` 본문만 LRU eviction한다. 쿼리와 merge는 시간·라벨 프루닝으로 선택된 part 본문만 검증된 임시 디렉터리로 내려받고 읽는 동안 eviction으로부터 pin한다. Parquet range read 최적화는 후속 단계로 남긴다.
- 장비 교체 (graceful shutdown): 1) SIGTERM 수신 시 ingest 엔드포인트를 즉시 차단(신규 요청 거부) 2) 이미 accept된 in-flight 요청의 WAL append/ack 완료까지 대기(drain) 3) 그 시점까지 쌓인 MemTable을 강제 flush하여 S3 업로드 및 manifest 갱신 완료를 확인 4) 프로세스 종료 후 디스크 폐기/새 장비 전환. 차단 이후 Alloy가 보내려던 데이터는 ack를 받지 못하므로 Alloy 자체 버퍼에서 재시도되며, 새 장비가 같은 엔드포인트로 서비스를 재개하면 그쪽으로 전달된다. drain 중 ack 직전에 연결이 끊기는 좁은 구간에서는 중복이 발생할 수 있으나(위 at-least-once와 동일한 성격), 유실은 발생하지 않는다.

### Object store 설정

- `LOGGYTRACY_OBJECT_STORE_URL`: `s3://bucket/prefix` 형식. 개발/테스트에는 단일 프로세스 전용인 `file:///absolute/path`도 사용할 수 있다. `file://` manifest 갱신은 프로세스 내부 직렬화와 atomic rename을 사용하며 다중 writer CAS를 제공하지 않는다. 미설정 시 로컬 전용 모드로 동작한다.
- S3 credential, region, endpoint, path-style 옵션은 `object_store`가 읽는 AWS/OBJECT_STORE 환경 변수를 사용한다.
- `LOGGYTRACY_CACHE_MAX_BYTES`: 로컬 Parquet 본문 캐시 상한(기본 10 GiB, 작은 catalog 파일은 제외). 오래 접근하지 않은 본문부터 제거하며, 이후 필요한 쿼리에서 manifest를 기준으로 다시 내려받는다.
- 시작 시 먼저 manifest catalog를 복구한다. 최초 object store 활성화로 manifest가 비어 있으면 로컬 tombstone 복구가 계산한 최종 active part 전체를 한 번의 CAS로 게시한다. 기존 manifest가 있으면 중단된 merge tombstone을 오래된 세대부터 재개한 뒤 일반 로컬 part를 업로드한다. 업로드 전에는 durable marker를 남겨 manifest 반영 전 중단된 작업을 다음 시작에서 검증·재개한다. marker 없이 완전한 원격 object set만 남은 part는 비활성 generation으로 판단해 되살리지 않는다. registry 밖의 로컬 디렉터리는 자동 삭제하지 않고 후속 retention 대상으로 보존한다.

### Ingest 입력 제한

모든 제한은 journal append **이전에** 적용되므로, 거절된 요청은 WAL에 아무 흔적을 남기지 않는다.
로그 라인 손실을 막는 쪽보다 엔진을 지키는 쪽을 우선한다 — 거절된 배치는 Alloy가 자체 WAL에서
재시도하거나 drop한다.

- `LOGGYTRACY_MAX_PUSH_BYTES` (기본 16 MiB): 압축된 push body 상한. axum의 암묵적 2 MiB 기본값을
  대체하므로, Alloy 배치 크기를 키울 때 이 값을 함께 올려야 한다.
- `LOGGYTRACY_MAX_DECOMPRESSED_PUSH_BYTES` (기본 64 MiB): snappy 헤더가 신고할 수 있는 압축 해제
  길이의 상한. `decompress_vec`는 스트림을 검증하기 전에 신고된 길이만큼 할당하므로, 이 검사가
  없으면 몇 바이트짜리 헤더가 할당 크기를 정한다.
- `LOGGYTRACY_MAX_LINE_BYTES` (기본 256 KiB)
- `LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM` (기본 30), `LOGGYTRACY_MAX_LABEL_NAME_BYTES` (기본 1 KiB),
  `LOGGYTRACY_MAX_LABEL_VALUE_BYTES` (기본 2 KiB): 스트림 카디널리티 폭발 방어. stream index는
  캐시 상한에서 제외되는 영속 카탈로그이므로, 카디널리티 폭발은 곧 evict 불가능한 디스크 사용량이 된다.
- `LOGGYTRACY_MAX_TIMESTAMP_AGE` (기본 7d), `LOGGYTRACY_MAX_TIMESTAMP_SKEW` (기본 1h):
  서버 시계 기준 수용 구간. `off`로 비활성화할 수 있다(과거 데이터 일괄 적재 시 필요).
  파티션이 UTC 일자 단위이므로 시계 오류나 단위 착오(초/밀리초를 나노초로 전송)가 파티션을 증식시키며,
  특히 **미래 날짜 part는 retention cutoff에 영구히 걸리지 않는다.**

- `LOGGYTRACY_DEFAULT_TENANT`, `LOGGYTRACY_MISSING_TENANT_POLICY`: 테넌트 식별 (위 "테넌시" 절).
  허용 목록 검증도 다른 제한과 같이 journal append 이전에 적용된다.

### Retention 설정

두 가지 모드가 있고 **섞이지 않는다**. 둘 다 설정하면 시작 시 검증 오류로 죽는다 —
retention 설정이 조용히 무시되는 것이 가장 나쁜 결과이기 때문이다.

- `LOGGYTRACY_RETENTION_PERIOD` (기본 미설정): 모든 테넌트에 적용되는 전역 기간.
- `LOGGYTRACY_TENANT_POLICY_TOKEN` (기본 미설정): 테넌트별 retention. 설정하면
  `PUT/GET/DELETE /loggytracy/api/v1/admin/tenants/{tenant}/retention` 라우트가 열리고,
  push된 정책이 유일한 권위가 된다. 미설정이면 라우트 자체가 없다. 함께 쓰는 값은
  `LOGGYTRACY_RETENTION_REWRITE_THRESHOLD` (기본 0.5 — part를 다시 쓸 만한 만료 행 비율).
  push된 값을 깎는 설정은 **없다**. 인스턴스가 상한을 걸면 미상 테넌트에는 닿지 않아,
  "영구 보존"이라고 명시한 테넌트가 아무 말도 없는 테넌트보다 데이터를 덜 갖게 된다
  (`RETENTION_DESIGN.md`의 "Rejected: an instance-side maximum").

정책은 테넌트당 객체 하나(`tenant_policies/<tenant>.json`)로 저장되고, 저장이 끝난 뒤에만
`200`을 준다. 실패하면 `503`이고 control plane이 재시도한다. 시작 시 전부 읽어들이며 실패는
치명적이다 — 매니페스트를 못 읽는 것과 같은 등급이다.

정책은 ingest·쿼리 hot path에 **없다**. 쓰기는 아예 조회하지 않고, 읽기는 정책이 없는 테넌트의
범위를 그대로 둔다(fail open).

이 제한들은 아직 전역이다. 테넌트별 스로틀·quota는 후속 작업이다(위 "테넌시" 절).

## LogQL 지원 범위 (subset)

지원 (사용 빈도 상위):

- 라벨 매처 `{app="x", env=~"prod|stage"}`
- line filter: `|=`, `!=`, `|~`, `!~`
- 파서: `| json`, `| logfmt`
- 라벨 필터: `| field="x"`, `| duration > 100ms` 등 비교 연산
- `| line_format`, `| label_format` (후순위)
- metric query: `rate`, `count_over_time`, `bytes_over_time`, `sum/avg/max/min by (...)`, `topk`
- `unwrap` + `quantile_over_time` (후순위)

미지원 문법은 파싱 단계에서 명확한 에러 메시지로 거부한다.

## 마일스톤

| 단계 | 내용 | 완료 기준 |
|---|---|---|
| M0 | axum + Loki push ingest + 저널(group commit, ack) + MemTable + LogQL 매처/line filter | Grafana Loki 데이터소스로 로그 조회 |
| M1 | Part flush (Parquet + trigram bloom + stream index) + 저널 복구 + MemTable/part 통합 쿼리 + merge | 재시작 후 데이터 유지, 프루닝 동작 확인 |
| M2 | object_store S3 업로드 + manifest(conditional write) + 디스크 캐시 eviction | 캐시 삭제 후에도 S3에서 쿼리 성공 |
| M3 | LogQL 확장: json/logfmt 파서, metric query, 필드 필터 push-down | 실제 대시보드 구동 |
| M4 | 트레이스: OTLP ingest + trace_id 조회(bloom) + Tempo API | Grafana Tempo 데이터소스로 트레이스 조회 |
| M5 | merge/컴팩션 튜닝, 리텐션, 자원 상한(쿼리 메모리·범위 제한), 부하 테스트 | 목표 처리량 달성 |
| M6 | graceful shutdown 기반 장비 교체 (SIGTERM 핸들러 + force-flush + drain-status readiness) | 장비 교체 리허설 성공 (손실 없이 신규 장비로 트래픽 전환) |
| M7 | 로컬 S3 부하 검증 (Tier B: 인프로세스 지연·장애 주입 스토어 / Tier C: 로컬 MinIO 실제 S3 프로토콜) + 부하 분석용 gauge 관측성 보강 | 목표 대비 처리량·지연·메모리·리텐션·에러율 검증 및 병목 문서화, MinIO에서 manifest CAS·원격 restore·retention GC 확인 |

## 참고

- VictoriaLogs `lib/logstorage` (Go, Apache 2.0 — 설계 참고용): part/block 구조, 타입 감지 인코딩, bloom 토크나이저, indexdb
- VictoriaTraces: 동일 스토리지 위 트레이스 구현 선례
- Quickwit: 오브젝트 스토리지 위 검색 아키텍처
- ClickHouse `ngrambf_v1`, Google Code Search: trigram 인덱스 기법

## 주요 크레이트

`tokio`, `axum`, `tonic`/`prost`(OTLP), `snap`(Loki push), `arrow`/`parquet`, `datafusion`(검토), `object_store`, `chumsky`(LogQL), `roaring`, `opentelemetry-proto`
