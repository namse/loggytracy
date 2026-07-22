# loggytracy 아키텍처

Rust로 만드는 싱글 머신 log + trace 엔진. VictoriaLogs의 논리 설계에 Parquet 물리 포맷과 S3 계층화를 결합한다.

## 확정된 결정

| 항목 | 결정 |
|---|---|
| 배포 형태 | 싱글 머신, 단일 writer |
| Source of truth | S3 호환 오브젝트 스토리지 |
| 로컬 디스크 | 캐시 (LRU eviction) |
| 내구성 | 저널(append-only) + group commit + fsync 후 ack. Alloy WAL을 안전망으로 전제 |
| 물리 포맷 | Parquet (dictionary + zstd) + 사이드카 인덱스 파일 |
| 인덱스 | stream index + 블록별 trigram bloom filter (역인덱스 없음) |
| 쿼리 언어 | LogQL — 사용 빈도 높은 subset만, 미지원 문법은 명확한 에러 |
| API | Loki HTTP API 호환 (Grafana Loki 데이터소스 직결), 트레이스는 Tempo API |
| Ingest 프로토콜 | Loki push (protobuf+snappy) + OTLP (gRPC) |

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
- 메타 파일 — 시간 범위, row 수, 필드 목록, min/max.

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
- 로컬 디스크는 part 캐시. eviction 후에도 S3 range read로 쿼리 가능.
- 장비 교체: 새 장비가 manifest를 읽어 복구, 미ack 데이터는 Alloy가 재전송.
- read replica: manifest 폴링으로 새 part 추적 (S3 반영 지연만큼 최신 데이터 지연). master 승격은 manifest generation 번호로 fencing.

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
| M6 | read replica + master 승격 | 장비 교체 리허설 성공 |

## 참고

- VictoriaLogs `lib/logstorage` (Go, Apache 2.0 — 설계 참고용): part/block 구조, 타입 감지 인코딩, bloom 토크나이저, indexdb
- VictoriaTraces: 동일 스토리지 위 트레이스 구현 선례
- Quickwit: 오브젝트 스토리지 위 검색 아키텍처
- ClickHouse `ngrambf_v1`, Google Code Search: trigram 인덱스 기법

## 주요 크레이트

`tokio`, `axum`, `tonic`/`prost`(OTLP), `snap`(Loki push), `arrow`/`parquet`, `datafusion`(검토), `object_store`, `chumsky`(LogQL), `roaring`, `opentelemetry-proto`
