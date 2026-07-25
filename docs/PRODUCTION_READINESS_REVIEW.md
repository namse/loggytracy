# 프로덕션 레디 리뷰 (2026-07-25)

리뷰 대상 리비전: `56afbbe` (M7 완료 시점, working tree clean)
리뷰 범위: `src/` 전체 (22,436 LOC), `docs/`, 배포 자산
테스트 상태: `cargo test` 211개 전부 통과

## 판정

**현재 상태로는 프로덕션 투입 불가.** 기능 완성도(LogQL subset, Loki/Tempo API, S3 계층화,
graceful shutdown, retention, merge)는 상당히 높고 크래시 복구 불변량은 코드와 주석 수준에서
꼼꼼하게 설계되어 있다. 그러나 다음 세 가지가 동시에 성립한다.

1. 오브젝트 스토어 백엔드에서 **flush 루프가 두 번째 compaction에서 영구 정지**한다 (재시작으로도 복구 불가).
2. **ingest backpressure가 전혀 없다.** flush가 멈춰도 계속 `204`를 반환하며 RAM/디스크가 무한 증가한다.
3. **테넌시가 구현되지 않았다.** `X-Scope-OrgID`를 파싱하지 않아 테넌트 간 데이터가 섞이고,
   테넌트별 스로틀·quota를 걸 대상 자체가 없다.

(1)과 (2)의 조합은 "장애 발생 → 자동 OOM/디스크 풀"을 보장한다. 이 세 개가 프로덕션 게이트다.

TLS는 이후 **범위 밖으로 확정**되었다 (`docs/ARCHITECTURE.md` "전송 보안 — TLS 미지원").
종단 암호화와 인증·인가는 리버스 프록시가 담당하며, 그 대신 리스닝 주소를 신뢰 경계 안에 두는 것이
배포 요구사항이 된다.

아래는 심각도별 전체 목록이다. `확인` 필드는 이 리뷰에서 실제로 재현/실행해 확인한 것과
코드 독해로 판단한 것을 구분한다.

## 수정 반영 이력

리뷰 이후 **입력 검증·설정·관측성 계열의 국소 수정**을 한 배치로 처리했다. 각 항목 본문에
`수정됨` / `부분 수정`으로 표시하고 남은 작업을 함께 적었다.

| 항목 | 상태 |
|---|---|
| P1-6 타임스탬프 수용 윈도우 | 수정됨 |
| P1-7 라벨/라인 크기 제한 | 부분 수정 (스트림 수 상한은 테넌시 대기) |
| P2-3 snappy 신고 길이 검증 + body 상한 knob | 수정됨 |
| P2-4 retention 전용 타임아웃 knob | 부분 수정 |
| P2-6 `RUST_LOG` 반영 | 수정됨 |
| P2-9 `file://` 경고 | 부분 수정 (opt-in 강제는 미적용) |
| TLS 미지원 / 테넌시 요구사항 문서화 | 완료 (`ARCHITECTURE.md`) |

배치에서 **의도적으로 제외한 것**: P0-1(WAL compaction)과 P0-2(backpressure)는 durability·
hot-path 변경이라 각각 전용 크래시 주입 테스트와 O(1) 크기 추적(P1-5)이 선행되어야 한다.
국소 수정과 섞으면 회귀 원인 추적이 어려워지므로 별도 작업으로 남겼다.

---

## P0 — 프로덕션 게이트 (이것 없이는 배포 금지)

### P0-1. WAL compaction이 두 번째 호출에서 영구 wedge (재시작으로도 복구 불가)

- 위치: `src/journal/compaction.rs:1-30`, `src/journal/replay.rs:27`
- 확인: **재현 완료** (임시 테스트 작성 후 원복)

`docs/M7_LOAD_RESULTS.md`가 이미 이 블로커를 기록하고 있으나, 리뷰 과정에서 **두 개의 서브 케이스와
재시작으로도 복구되지 않는다는 사실**을 추가로 확인했다.

성공한 compaction 후 `journal.wal.compact.state`(phase=2)가 삭제되지 않고 남는다. compaction은 WAL을
잘라내고 checkpoint를 0으로 리셋하므로 이후 offset은 **새 좌표계**에 산다. 다음 compaction은 옛 좌표계의
stale offset과 비교되어 세 갈래로 갈린다.

| 다음 offset | 코드 경로 | 결과 |
|---|---|---|
| `< state.offset` | `compaction.rs:22-27` | `Err("WAL compaction checkpoint moved backwards")` → **영구 wedge** |
| `== state.offset` | `compaction.rs:12-20` | 조용히 no-op 반환. **WAL이 잘리지 않음** (M7 문서에 없던 케이스) |
| `> state.offset` | fall-through | 우연히 정상 동작 |

재현 결과 (첫 레코드 32B, 두 번째 31B):

```
first_offset=32 wal_after_first=0 second_offset=31
result=Err(Custom { kind: InvalidInput, error: "WAL compaction checkpoint moved backwards" })
wal_after_second=31
```

두 번째 배치가 첫 배치보다 조금이라도 작으면 즉시 wedge된다. 실 트래픽에서는 거의 확정적으로 발생한다.

**M7 문서에 없는 추가 사실 — 재시작으로 복구되지 않는다.** `recover_unfinished_compaction`은
`replay.rs:27`에서 `state.phase != 1`이면 즉시 return하므로, 재시작해도 phase=2 state 파일이
그대로 남아 같은 wedge를 다시 만난다. 유일한 복구 수단은 운영자가 `journal.wal.compact.state`를
수동 삭제하는 것이다. 이 사실이 문서화되어 있지 않다.

wedge 이후 동작: `flush.rs:52-91`이 매 tick 같은 doomed offset을 재시도 → 항상 실패 → `continue`로
새 flush 진입 자체가 차단. `part_count` 동결, WAL·MemTable 무한 증가, `/ready` 503, 그런데
**ingest는 계속 204를 반환**한다.

**수정 방향**
- compaction 성공 직후 state 파일을 제거하고 그 제거를 durable하게(부모 디렉터리 fsync) 만든다.
- 재시작 시 살아남은 phase=2 state는 "이미 완료"로 간주해 제거한다 (`replay.rs`의 early-return 수정).
- absolute offset 비교를 좌표계 세대(generation) 개념으로 교체하는 것을 권한다. 현재는 세 갈래
  분기 전부가 "offset이 같은 좌표계에 있다"는 무효한 전제에 서 있다.
- 테스트 게이트: **연속 2회 이상 성공 compaction** 케이스가 현재 테스트에 없다
  (`src/journal/tests.rs`의 compaction 테스트 4개 모두 단일 compaction). 크기가 감소/동일/증가하는
  세 케이스 + 각 크래시 지점 주입 테스트를 추가해야 한다.

### P0-2. ingest backpressure 부재 — 장애 시 무한 증가 후 OOM

- 위치: `src/ingest.rs:17` (draining 체크만 존재), `src/memtable.rs:92`, `src/config.rs` (관련 knob 없음)
- 확인: 코드 독해 + P0-1 재현 시 동작 확인

`push`는 `is_draining()`만 검사한다. 다음 어떤 것도 ingest를 막지 않는다.

- MemTable 바이트 상한 없음
- WAL backlog 상한 없음 (`loggytracy_wal_backlog_bytes`는 노출만 하고 gating에 미사용)
- flush 연속 실패 카운트 기반 차단 없음
- `429 Too Many Requests` 경로가 코드 전체에 존재하지 않음

따라서 S3 장애든 P0-1이든 flush가 멈추면 서버는 계속 `204`로 ack하면서 RAM과 디스크를 소진한다.
`/ready`가 503으로 바뀌지만 Alloy는 `/ready`를 보지 않으므로 아무 효과가 없다. M7 런에서
`wal_backlog_bytes=19.9 MB`로 상한을 넘긴 것이 이 경로다.

**수정 방향**
- `LOGGYTRACY_MAX_MEMTABLE_BYTES`, `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES` 도입. 초과 시 journal append
  **이전에** `429`(재시도 유도) 반환. Alloy는 429에 backoff하므로 클라이언트 WAL이 안전망 역할을 한다.
- soft/hard 2단 임계값을 권한다: soft에서 `Retry-After`와 함께 429, hard에서 503.
- `ARCHITECTURE.md`의 "Alloy WAL을 안전망으로 전제"는 서버가 거절 신호를 줄 때만 성립한다.
  현재는 서버가 ack해버리므로 그 전제가 깨져 있다.

### P0-3. 멀티테넌시 미구현 — 테넌트별 스로틀·quota의 전제가 없음

- 위치: `src/router.rs` 전체 (미들웨어 없음), `src/ingest.rs`, `src/trace_ingest.rs`
- 확인: `rg`로 tenant 관련 코드 전무 확인

**TLS는 범위 밖으로 확정되었다** (`docs/ARCHITECTURE.md`의 "전송 보안 — TLS 미지원"). 종단 암호화와
인증·인가는 리버스 프록시가 담당한다. 따라서 이 항목의 게이트는 **테넌시**다.

`X-Scope-OrgID`를 **파싱하지 않는다.** Loki/Tempo 데이터소스와 Alloy가 테넌트 헤더를 보내도 전부
한 네임스페이스에 섞인다. 결과:

- 테넌트 A의 로그가 테넌트 B의 쿼리에 그대로 나온다 (조용한 데이터 유출).
- **테넌트별 스로틀·quota를 걸 대상이 존재하지 않는다.** 한 테넌트의 폭주가 전체를 죽인다.
  이것이 P0-2(backpressure 부재)와 결합하면 "한 테넌트가 서버를 OOM시킬 수 있다"가 된다.
- 테넌트별 retention, 용량 회계, 삭제 요청 대응이 모두 불가능하다.

**수정 방향** (아키텍처 문서의 "테넌시" 절이 목표 설계)
- ingest·쿼리 양쪽에서 `X-Scope-OrgID`를 추출한다. OTLP는 gRPC 메타데이터의 동명 키.
  헤더 부재 시 정책(기본 테넌트 수용 vs 거절)을 설정으로 노출한다.
- 테넌트를 **저장 경로의 분할 축**으로 넣는다 (manifest/part 경로). 스트림 라벨로 처리하면
  테넌트별 회계와 삭제가 전체 스캔이 된다.
- quota 대상: ingest rate, 활성 스트림 수, 저장 용량, 동시 쿼리, 쿼리 스캔 예산.
  초과 시 ingest `429` (Alloy가 backoff + 자체 WAL로 버팀).
- 모든 quota/거절 카운터에 테넌트 라벨을 붙여 `/metrics`에 노출한다 (P2-7의 라벨 부재와 함께 처리).

**참고 — 프록시 신뢰 전제**: 엔진은 `X-Scope-OrgID`를 검증 없이 신뢰한다. 헤더를 위조할 수 있는
네트워크 위치에서 엔진에 직접 접근이 가능하면 테넌트 격리가 무너진다. 리스닝 주소를 신뢰 경계
안에 두는 것이 배포 요구사항이며, `0.0.0.0` 기본 바인드(`src/config.rs`)는 이 요구사항과 어긋난다.

---

## P1 — 실사용 시 곧 문제가 되는 것

### P1-1. Tempo search가 전체 trace part를 S3에서 복원한 뒤 전량 스캔

- 위치: `src/tempo/handlers.rs:96, 159, 185` → `src/tempo/scan.rs:114` `pin_all_trace_parts`
- 확인: 코드 독해 (`state.trace_parts.part_ids()` = 전체 ID 집합)

`trace_by_id`는 bloom pruning(`candidate_part_ids`)을 제대로 쓴다. 그러나 `search`,
`search_tags`, `search_tag_values` 세 엔드포인트는 모두 `pin_all_trace_parts`를 호출해
**시간 범위와 무관하게 모든 trace part 본문을 로컬로 복원**한 뒤 전량 스캔한다.
`search`의 start/end 필터는 스캔이 끝난 후 `handlers.rs:130`에서 적용된다.
`search_tags` / `search_tag_values`는 시간 범위 파라미터 자체가 없다.

Grafana의 Tempo 데이터소스는 태그 드롭다운을 열 때마다 `search/tags`를 호출한다. 즉
**UI를 한 번 열면 전체 trace 데이터셋이 S3에서 다운로드된다.** 캐시 상한(`cache_max_bytes`)을
넘으면 다운로드 → eviction → 재다운로드가 반복되어 S3 요청 비용과 대역폭이 폭증한다.

**수정 방향**: trace part meta의 `min_ts_ns`/`max_ts_ns`로 시간 프루닝을 pin 단계에서 적용하고,
tags/tag_values에 시간 범위 파라미터를 지원한다(Tempo API에 `start`/`end`가 있다). 태그
카탈로그는 part 사이드카에 미리 집계해 본문 복원 없이 답하는 것이 정석이다.

### P1-2. OTLP 로그 수집이 구현되지 않았는데 문서는 지원한다고 기술

- 위치: `src/startup.rs:342` — `add_service(otlp_service.into_server())`가 `TraceServiceServer` 하나뿐
- 확인: 코드 독해 (`LogsService` 구현·등록 없음)

`docs/ARCHITECTURE.md`는 "Ingest 프로토콜: Loki push (protobuf+snappy) + OTLP (gRPC)"라고 적고,
데이터 모델은 "로그와 스팬 모두 wide event로 통일"이라고 기술한다. 그러나 실제 등록된 gRPC 서비스는
trace 하나이며, Alloy가 `otelcol.exporter.otlp`로 **로그**를 보내면 `UNIMPLEMENTED`가 난다.
OTLP/HTTP(`/v1/traces`, `/v1/logs`)도 없다 — Alloy 구성에서 `otlphttp`가 흔한 선택지다.

**수정 방향**: 구현하거나(권장: LogsService + OTLP/HTTP 라우트), 문서에서 "로그는 Loki push 전용"으로
정정한다. 문서와 구현의 불일치 자체가 도입 단계에서 신뢰를 깎는다.

### P1-3. group commit이 항상 batch 타이머를 소진 — 커넥션당 ~5 push/s 상한

- 위치: `src/journal/writer.rs:230-231`
- 확인: 코드 독해 + `docs/M7_LOAD_RESULTS.md`의 측정치와 일치

배치 루프는 `batch_bytes < max_batch_bytes`인 동안 `timeout_at(deadline, rx.recv())`로 대기한다.
후속 요청이 없어도 **`max_batch_ms`(기본 200ms) 전체를 기다린다.** 따라서 모든 push의 ack 지연은
사실상 200ms 고정이고, 단일 커넥션 처리량은 ~5 push/s로 고정된다.

M7 문서는 이를 "클라이언트 아티팩트"로 분류했지만, 서버가 채널이 빈 것을 알면서도 타이머를
소진하는 것은 서버 측 설계 문제다. 정석 group commit은 **즉시 write를 시작하고, fsync 진행 중에
도착한 요청이 다음 배치를 만든다.** 현재 구현은 도착을 기다렸다가 write한다.

영향: Alloy 인스턴스 수가 적은 환경(소규모 배포, 단일 노드 k8s)에서 처리량 상한이 매우 낮다.
`max_batch_ms`를 낮추면 fsync 횟수가 늘어 디스크가 병목이 되는 trade-off로 밀린다.

**수정 방향**: 첫 레코드 도착 즉시 write+fsync 시작, fsync 중 도착분을 다음 배치로 모으는 구조로 전환.
`max_batch_ms`는 상한이 아니라 fsync가 즉시 끝날 때의 상한으로만 작동해야 한다.

### P1-4. 단일 writer 가정을 강제하는 장치가 없음 (split-brain)

- 위치: `src/object_storage/catalog.rs` (lease/fencing token 없음)
- 확인: 코드 독해

아키텍처는 "싱글 머신, 단일 writer"를 전제하지만 이를 **강제하는 메커니즘이 없다.** 같은
`LOGGYTRACY_OBJECT_STORE_URL` prefix로 두 프로세스를 띄우면 둘 다 정상 동작한다고 믿고 쓴다.
manifest CAS가 lost update는 막지만 다음을 막지 못한다.

- 두 프로세스의 로컬 WAL/캐시가 서로 다른 히스토리를 가진다.
- 각자의 retention이 상대가 방금 등록한 part를 만료 대상으로 판단할 수 있다.
- M6의 장비 교체 절차에서 구 인스턴스가 완전히 죽기 전에 신 인스턴스가 뜨면 정확히 이 상태가 된다.

M6 절차가 순서를 잘 정의해두었지만, 순서를 지키게 **강제**하는 것과 문서로 부탁하는 것은 다르다.
운영 자동화(k8s rolling update 등)가 절차를 어기는 것은 흔한 일이다.

**수정 방향**: manifest에 writer epoch/lease를 넣고, CAS 시 자신의 epoch를 검증한다. 다른 epoch가
관측되면 즉시 self-fence(ingest 거부 + 프로세스 종료). 오브젝트 스토어만으로 구현 가능하다.

### P1-5. MemTable flush가 전체 스냅샷을 deep clone, size 계산이 O(rows)

- 위치: `src/memtable.rs:98-113` (`begin_flush`의 `snapshot.clone()`), `src/memtable.rs:135-170`
- 확인: 코드 독해

두 가지 문제가 겹친다.

1. `begin_flush`가 `snapshot.clone()`으로 전체 엔트리를 복제한다 → flush 순간 메모리 2배.
2. `approximate_size()`가 모든 스트림의 모든 엔트리를 순회한다. `flush_loop`이 이를
   `flush_check_interval`(기본 500ms)마다 호출하고, `finalize_flush`는 루프마다 호출한다.

정상 시(1 MiB memtable)에는 무해하다. 문제는 **장애 시**다. memtable이 커질수록 500ms마다의
O(rows) 순회가 CPU를 태우고, 그 순회가 `inner` RwLock read를 잡고 있는 동안 `insert`의
write lock이 대기한다 → ingest 지연이 memtable 크기에 비례해 악화된다. 즉 P0-2의 무한 증가
시나리오에서 지연이 선형으로 나빠지며 상황을 가속한다.

**수정 방향**: `AtomicU64` 누적 카운터로 크기를 O(1) 추적한다(insert/commit/abort에서 갱신).
`begin_flush`의 clone은 `Arc`로 공유하거나 flushing 버퍼를 move + 필요 시 재삽입으로 바꾼다.

### P1-6. 타임스탬프 수용 윈도우가 없어 파티션이 무한 증식 — **수정됨**

- 위치: `src/part/format.rs:1-5` (`partition_of`), `src/ingest.rs:78` (i64 범위만 검증)
- 확인: 코드 독해

ingest는 타임스탬프가 i64 나노초 범위에 드는지만 본다. 파티션은 UTC 일자 단위이므로,
시계가 틀린 클라이언트나 **단위 착오**(초/밀리초를 나노초로 보내는 매우 흔한 실수)가
수천 개의 파티션 디렉터리를 만든다. 게다가

- 미래 날짜 part는 retention cutoff(`max_ts_ns < cutoff`)에 걸리지 않아 **영구히 남는다.**
- `DateTime::from_timestamp(...).unwrap_or_default()`는 변환 실패를 조용히 `1970-01-01`로 매핑한다.

Loki에는 `reject_old_samples`, `creation_grace_period`가 있다. 여기에는 동등한 방어가 없다.

**수정 완료**: `LOGGYTRACY_MAX_TIMESTAMP_AGE`(기본 7d) / `LOGGYTRACY_MAX_TIMESTAMP_SKEW`(기본 1h)를
도입해 journal append 이전에 400으로 거절한다(`src/ingest.rs`의 `TimestampWindow`). `off`로
비활성화하면 과거 데이터 일괄 적재가 가능하다.

**남은 작업**: 거절 카운터를 metrics에 노출하는 것은 P2-7(라벨 있는 metric)과 함께 처리한다.
`partition_of`의 `unwrap_or_default()`는 i64 나노초 범위 안에서는 항상 유효한 날짜를 만들므로
실질 위험이 없어 그대로 두었다.

### P1-7. 라벨/라인/스트림 카디널리티 제한 전무 — **부분 수정**

- 위치: `src/proto.rs:90-140` (`parse_labels`), `src/memtable.rs:92`
- 확인: 코드 독해

Loki가 가진 다음 제한들이 전혀 없다.

| Loki 제한 | loggytracy |
|---|---|
| `max_label_names_per_series` (30) | `max_label_names_per_stream` ✓ |
| `max_label_name_length` (1024) | `max_label_name_bytes` ✓ |
| `max_label_value_length` (2048) | `max_label_value_bytes` ✓ |
| `max_line_size` | `max_line_bytes` ✓ |
| `max_streams_per_user` | **없음** — 테넌시 필요 |
| `max_entries_limit_per_query` | `max_log_limit`으로 있음 ✓ |

라벨 하나에 request ID를 실수로 넣은 클라이언트 하나가 memtable 스트림 HashMap과 part의
stream index를 폭발시킬 수 있다. stream index는 캐시 상한에서 제외되는 "작은 영속 카탈로그"로
설계되어 있어(`cache.rs`의 `CATALOG_FILES` 주석) 카디널리티 폭발이 곧 **evict 불가능한
디스크 사용량**이 된다.

**수정 완료**: 라벨 개수·이름 길이·값 길이·라인 크기 상한을 `src/ingest.rs`의 `validate_labels`와
라인 검사로 적용한다. 모두 journal append 이전이므로 거절된 요청은 WAL에 남지 않는다.

**남은 작업**: 활성 스트림 수 상한(`max_streams_per_user`)은 **테넌시 없이는 의미가 없다** —
전역 상한은 한 테넌트가 다른 테넌트의 ingest를 막는 도구가 되어버린다. P0-3과 함께 처리한다.

### P1-8. merge 기본 설정이 서로 모순 — 대형 그룹 merge가 영구 실패

- 위치: `src/merge/selection.rs:31-76` (`group_for_merge`), `src/merge/selection.rs:89-120`
- 확인: 코드 독해 + 기본값 계산

그룹 선택은 `estimated_part_bytes`(= **압축된** parquet 파일 크기)를
`merge_max_input_bytes`(512 MiB)와 비교한다. 그런데 실제 읽기는
`read_all_rows_with_limit`이 **압축 해제된** row 바이트를 `merge_max_memory_bytes`(1 GiB)와
비교한다. zstd + dictionary는 로그 텍스트에서 5~20배가 흔하므로 두 상한은 압축률만큼 어긋나 있다.

행 수 기준으로도 위험하다. `merge_target_part_rows` = 1,000,000이고 1 KiB 라인이면
압축 해제 크기는 약 1 GiB — 기본 상한에 딱 걸린다. `merge_max_part_rows` = 4,000,000까지
그룹이 자라면 확실히 초과한다.

초과 시 `Err("merge exceeds the maximum of ... materialized bytes")`가 나고 `merge_once`는
`continue`한다. 그러나 **다음 tick에 같은 그룹이 같은 방식으로 선택되어 같은 이유로 실패한다.**
더 작은 그룹으로 물러서는 fallback이 없다. 결과: merge 영구 실패 → part 수 무한 증가 →
쿼리 계획 비용 증가 → `merge_healthy=false`로 `/ready` 503 고착.

**수정 방향**
- 두 상한을 같은 단위로 통일하거나, 그룹 선택 시 압축률 추정치(part meta에 uncompressed size 기록)를 쓴다.
- 메모리 초과 실패 시 그룹을 절반으로 쪼개 재시도하는 fallback을 넣는다.
- 근본적으로는 merge를 streaming k-way merge로 바꿔 전량 materialize를 없앤다
  (`todo.md` P2의 "Parquet range read"와 같은 축).

### P1-9. 캐시 eviction이 registry write lock을 잡고 동기 디렉터리 순회

- 위치: `src/startup.rs:242-250`, `src/object_storage/cache.rs`의 `evict_cache`/`evict_trace_cache`
- 확인: 코드 독해

eviction 워커는 `registry.operation_lock().write_owned()`를 잡은 뒤 `evict_cache`를 호출한다.
`evict_cache`는 `async` 함수가 아니고 `spawn_blocking`으로도 감싸지 않은 **동기 함수**이며,
parts 트리 전체를 `read_dir` + 항목마다 `symlink_metadata`로 순회한다.

두 가지가 동시에 나쁘다.

1. 그 순회 시간 내내 **모든 쿼리·flush·merge·retention이 차단**된다 (write lock).
2. tokio 워커 스레드를 블로킹한다 (기본 30초마다).

part 수가 수만 개가 되면 순회당 수만 번의 stat syscall이 발생한다. 이것이 쿼리 p99에
주기적인 스파이크로 나타난다.

**수정 방향**: `spawn_blocking`으로 옮기고, 크기·access time 정보를 registry에 인메모리로
유지해 디스크 순회 자체를 없앤다. write lock은 실제 파일 삭제 구간만 잡는다.

### P1-10. 시작 시 오브젝트 스토어 오류가 곧 패닉 → S3 일시 장애에 crash loop

- 위치: `src/startup.rs:83, 87, 91, 95, 109`
- 확인: 코드 독해

startup은 object store 초기화, flush transaction 복구, local cache reconcile, trace reconcile
전부를 `panic!`으로 처리한다. **일시적 네트워크 오류와 실제 데이터 손상을 구분하지 않는다.**
S3가 몇 초 흔들리는 동안 프로세스가 재시작하면 그대로 패닉하고, 오케스트레이터가 재시작하면
다시 패닉한다 → crash loop. crash loop 동안 ingest는 완전 중단이고, 그 사이 Alloy WAL이
차오른다.

**수정 방향**: 복구 가능한 I/O 오류는 backoff 재시도하고 그 동안 `/ready`를 503으로 유지한다
(리스너는 띄워 `/ready`가 응답 가능해야 한다). 진짜 무결성 위반(manifest 형식 오류, 검증 실패)만
패닉으로 남긴다.

### P1-11. 시작 시간과 flush 비용이 part 수에 선형

- 위치: `src/object_storage/cache.rs`의 `restore_catalog`, `src/object_storage/catalog.rs`의 manifest CAS
- 확인: 코드 독해

두 가지 O(N) 경로가 있다.

1. `restore_catalog`이 manifest의 **모든** part에 대해 로컬 존재 확인 후 없으면 카탈로그 파일을
   **순차** 다운로드한다. 병렬화가 없다. part 10,000개면 왕복 수만 번이 직렬로 일어난다.
   더구나 `reconcile_local_cache`는 `restore_catalog`을 시작과 끝에 두 번 호출하고, merge 그룹마다
   `load_manifest()`를 다시 호출한다.
2. manifest는 **모든 part를 담은 단일 JSON**이고 flush마다(기본 5초) 전체가 CAS로 재작성된다.
   part 10,000개면 매 5초마다 수 MB를 PUT한다. S3 요청 비용과 CAS 충돌 확률이 함께 오른다.

`docs/M7_LOAD_RESULTS.md`의 런은 part가 3개였으므로 이 축은 전혀 검증되지 않았다.

**수정 방향**: 카탈로그 다운로드 병렬화(bounded concurrency), `reconcile_local_cache`의 중복
`load_manifest`/`restore_catalog` 제거. 중기적으로는 manifest를 세대별 delta + 주기적 스냅샷
구조로 바꿔 flush당 쓰기량을 O(변경분)으로 만든다.

---

## P2 — 운영 품질 / 호환성

### P2-1. Loki API 호환 공백

- 위치: `src/router.rs`
- 확인: 코드 독해 + Loki API 대조

Grafana Loki 데이터소스가 호출하는데 없는 엔드포인트:

| 엔드포인트 | 영향 |
|---|---|
| `/loki/api/v1/tail` (WebSocket) | Grafana **Live tail 동작 불가** |
| `/loki/api/v1/index/volume`, `volume_range` | Explore의 volume 히스토그램 실패 |
| `/loki/api/v1/patterns` | 패턴 탐색 실패 |
| `/loki/api/v1/detected_fields`, `detected_labels` | Grafana 11+ 필드 탐색 실패 |
| `/loki/api/v1/format_query` | 쿼리 포맷 버튼 실패 |
| `/loki/api/v1/delete` (삭제 API) | GDPR/삭제 요청 대응 불가 |

동작하지만 부정확한 것:

- `labels`, `label_values`, `series`, `index_stats`가 **`start`/`end`를 완전히 무시**한다
  (`handlers.rs:199, 213, 290, 426`). Grafana는 항상 시간 범위를 보내므로, 드롭다운에
  "그 범위에 존재하지 않는 라벨"까지 전부 나온다. 동시에 매 요청이 전체 히스토리를 훑는다.
- `label_values`가 Loki의 `query` 파라미터(매처로 값 필터링)를 지원하지 않는다.
- JSON push가 `415 "JSON push not supported in M0"`로 거절된다 — 에러 메시지에 마일스톤
  이름이 남아 있고, Promtail/일부 SDK는 JSON push를 쓴다.
- `buildinfo`가 `revision: "unknown"`, `branch: "main"`을 하드코딩한다 (`handlers.rs:281`).
  배포된 리비전을 확인할 방법이 없다.

Tempo 쪽도 v2 API(`/api/v2/search/tags`, `/api/v2/search/tag/{tag}/values`)와 `/api/echo`가 없다.
최신 Grafana Tempo 데이터소스는 v2를 먼저 시도한다.

### P2-2. 메타데이터 엔드포인트에 리소스 가드 없음

- 위치: `src/query/handlers.rs:199, 213, 290, 426`
- 확인: 코드 독해

로그/메트릭 쿼리 경로는 semaphore + 타임아웃 + scan 예산 + 메모리 예산을 잘 갖췄다. 그런데
`labels`, `label_values`, `series`, `index_stats`는 **semaphore도, 타임아웃도, 범위 검증도 없다.**
`series`는 `match[]`를 개수 제한 없이 받아 매처마다 전체 part를 훑는다. P0-3(무인증)과 결합하면
가장 값싼 DoS 경로다.

### P2-3. snappy 압축 해제가 신고된 길이를 그대로 할당, body 상한이 설정 불가 — **수정됨**

- 위치: `src/ingest.rs:54`
- 확인: `snap-1.1.2` 소스 확인 (`decompress.rs`의 `vec![0; decompress_len(input)?]`)

`decompress_vec`는 검증 전에 **헤더가 신고한 길이만큼 즉시 할당**한다. snappy의
`MAX_INPUT_SIZE`는 `u32::MAX`이므로 수 바이트짜리 varint 헤더로 최대 4 GiB 할당을 유발할 수 있다.
`MAX_RECORD_BYTES`(256 MiB) 검증은 압축 해제 **후에** 일어난다.

리눅스 기본 overcommit에서는 `vec![0; n]`이 lazy zero page라 RSS 영향이 제한적이므로 즉시
치명적이지는 않다. 그러나 overcommit을 끈 환경이나 VA 압박 상황에서는 실패하며, 무엇보다
"신뢰할 수 없는 입력이 할당 크기를 정한다"는 자체가 고쳐야 할 패턴이다.

함께 있는 문제: **body 크기 상한이 설정 불가**하다. axum의 암묵적 기본값 2 MiB를 그대로 쓰므로
Alloy의 배치 크기를 키운 운영자는 원인 불명의 `413`을 만나고, 조절할 env knob이 없다.

**수정 완료**: `snap::raw::decompress_len`으로 신고 길이를 먼저 확인해
`LOGGYTRACY_MAX_DECOMPRESSED_PUSH_BYTES`(기본 64 MiB)를 넘으면 `413`으로 거절한다.
`DefaultBodyLimit`을 `LOGGYTRACY_MAX_PUSH_BYTES`(기본 16 MiB)로 노출했고, 핸들러도 같은 상한을
검사해 axum의 무정보 413 대신 구체적인 에러 메시지를 준다.

### P2-4. retention이 로컬 본문을 manifest CAS보다 먼저 삭제 — **부분 수정**

- 위치: `src/retention.rs:130-165`
- 확인: 코드 독해

로컬 part 디렉터리를 먼저 지우고(write lock 안) 그 다음 remote manifest CAS를 한다. 크래시
안전성은 주석대로 확보되지만, **CAS가 실패하면** 해당 part는 registry에 등록된 채 manifest에도
살아있고 로컬 본문만 없는 상태로 남는다(`removed_log_ids`의 `unregister`는 CAS 성공 후에만 실행).
다음 retention 패스까지 그 part를 복원해야 하는 쿼리는 실패한다. 수렴은 하지만 그 사이 쿼리
결과가 에러가 된다.

부수 문제:
- ~~retention/GC 타임아웃에 `config.max_restore_runtime`(25초)을 재사용한다~~ →
  **수정됨**: `LOGGYTRACY_MAX_RETENTION_RUNTIME`(기본 120초)을 분리했다. 기존 25초는 전체 prefix를
  LIST하는 GC에는 너무 짧아 대규모 버킷에서 타임아웃 실패를 반복할 값이었다.
- `garbage_collect_orphans`가 매번 `parts`와 `trace_parts` prefix **전체를 LIST**한다.
  object 수에 선형인 LIST 비용이 retention이 뭔가 지울 때마다 발생한다.
- `retention_period` 기본값이 `None`(무한)이다. 기본 설정 배포는 S3와 디스크가 영원히 자란다.

### P2-5. 크래시 후 중복 로그가 관측 불가능

- 위치: `docs/ARCHITECTURE.md`의 at-least-once 항목, `todo.md` P2
- 확인: 문서 + 코드 독해

at-least-once 트레이드오프 자체는 의식적 결정으로 문서화되어 있다. 문제는 **중복이 발생했는지
알 방법이 없다는 것**이다. 크래시 복구 시 재생된 레코드 수를 세는 metric도, 경고 로그도 없다.
운영자는 "지금 보고 있는 count_over_time 결과가 중복 때문에 부풀려진 것인지" 판단할 근거가 없다.

**수정 방향**: dedup 구현 전이라도 `loggytracy_replay_records_total`,
`loggytracy_replay_duplicate_window_bytes` 같은 gauge를 노출하고, 복구 시 WARN 로그로
"이 시점 이후 구간에 중복이 있을 수 있음"을 남긴다.

### P2-6. 로그 레벨 하드코딩 — `RUST_LOG` 무시 — **수정됨**

- 위치: `src/main.rs:40` — `.with_env_filter("loggytracy=debug,info")`
- 확인: 코드 독해

프로덕션에서 `loggytracy=debug`가 강제되며 운영자가 바꿀 수 없다. 로그 양·비용 문제이기도 하고,
장애 시 임시로 trace 레벨을 올리는 것도 불가능하다.

**수정 완료**: `EnvFilter::try_from_default_env()`로 `RUST_LOG`를 반영하고, 미설정 시 기본값을
`loggytracy=info,warn`으로 낮췄다.

### P2-7. `/metrics`로 SLO를 계산할 수 없음

- 위치: `src/metrics.rs`, `src/query/handlers.rs:299-420`
- 확인: 코드 독해

- 히스토그램/summary가 없다. 지연은 `*_latency_ns_total` 누적합뿐이라 **p95/p99를 구할 수 없다.**
  M5/M7 목표표가 p95/p99로 쓰여 있는데 운영 중에는 그 지표를 볼 수 없다.
- 라벨이 전혀 없다. 엔드포인트별·상태코드별 에러율을 낼 수 없다.
- `# HELP`가 없다.
- 버전/리비전 정보 metric이 없다.
- `merge_debt_part_count`가 매 스크레이프마다 `estimated_part_bytes` → part마다 `fs::metadata`를
  호출한다(`selection.rs:78-82`). part 수만큼의 stat syscall이 스크레이프 주기마다 발생한다.

### P2-8. shutdown의 운영자 abort가 컨테이너 환경에서 동작하지 않음

- 위치: `src/shutdown.rs`의 `spawn_abort_watcher`, `startup.rs`의 종료 시퀀스
- 확인: 코드 독해 (코드 주석도 일부 인정하고 있음)

force-flush는 하드 타임아웃 없이 무한 재시도하고, 유일한 탈출구가 **stdin에 `exit` 입력**이다.
systemd나 컨테이너에서 stdin은 TTY가 아니므로 이 경로는 사용 불가다(코드도
"stdin is unavailable; operator-initiated shutdown abort is disabled"로 인지하고 있다).

그 결과 컨테이너 환경의 실제 동작은 이렇게 된다. S3가 죽은 상태에서 SIGTERM →
force-flush 무한 재시도 → 오케스트레이터의 `terminationGracePeriodSeconds`(기본 30초) 만료 →
**SIGKILL**. 데이터는 WAL에 있으므로 손실은 없지만, 오케스트레이터는 그 다음 파드를
**다른 노드에 스케줄**할 수 있고 그러면 그 디스크가 버려진다. 즉 M6가 막으려던 손실이
정확히 그 경로로 발생한다.

`startup.rs`가 abort 시 exit code 1을 반환하는 것은 좋은 설계지만, **SIGKILL에는 exit code가 없다.**

**수정 방향**: stdin 대신 관리용 엔드포인트나 시그널(SIGUSR1)로 abort를 받는다. 더 중요하게,
운영 문서에 "이 워크로드는 StatefulSet + 고정 PV여야 하고 `terminationGracePeriodSeconds`는
사실상 무한이어야 한다"를 명문화하고, `/ready` + `pending_flush_bytes`를 보고 교체를 진행하는
컨트롤러 절차를 제공해야 한다.

### P2-9. `file://` 백엔드가 CAS 없이 overwrite — 프로덕션 오사용 방어 없음 — **부분 수정**

- 위치: `src/object_storage/catalog.rs`의 `local_manifest_overwrite`
- 확인: 코드 독해

`file://` 스킴은 `PutMode::Overwrite`로 폴백한다(프로세스 내 mutex + rename에 의존).
주석은 "single-process development backend"라고 명시하지만, **런타임에 아무 경고도 없다.**
누군가 NFS 마운트를 `file://`로 지정하면 조용히 CAS 없는 manifest 갱신이 되고, P1-4의
split-brain과 결합해 manifest lost update가 발생한다.

**수정 완료**: `ObjectStorage::from_url`이 `file://` 스킴을 감지하면 CAS 없이 overwrite를 쓴다는
사실과 공유/네트워크 스토리지에 쓰지 말라는 경고를 WARN으로 남긴다.

**남은 작업**: 명시적 opt-in(`LOGGYTRACY_ALLOW_UNSAFE_LOCAL_STORE`) 요구는 부하 하네스 스크립트가
`file://`에 의존하므로 함께 정리해야 한다.

### P2-10. 과부하 신호(429)가 없어 Alloy가 backoff할 수 없음

- 위치: `src/ingest.rs`, `src/trace_ingest.rs`
- 확인: 코드 독해

현재 응답 매핑은 파싱 실패 → 400(Alloy가 drop, 적절), journal 실패 → 500(Alloy가 재시도, 적절)이다.
그런데 **과부하 상태를 표현하는 429가 없다.** 서버가 감당 못 하는 상황에서 클라이언트에게
"천천히 보내라"고 말할 방법이 없고, 그래서 P0-2의 무한 증가가 완화되지 않는다.
OTLP 쪽도 `RESOURCE_EXHAUSTED`를 크기 초과에만 쓰고 부하에는 쓰지 않는다.

---

## P3 — 배포·문서 자산 부재

확인: `ls` (해당 파일들이 존재하지 않음)

| 항목 | 상태 |
|---|---|
| Dockerfile | 없음 |
| k8s manifest / helm chart | 없음 |
| systemd unit | 없음 |
| 설정 레퍼런스 문서 | 없음 — 40여 개 env knob이 `src/config.rs`에만 존재 |
| 운영 runbook | 부분 (ARCHITECTURE.md의 shutdown 절차만) |
| 백업·DR 절차 | 없음 (S3가 source of truth인데 버전관리/복제 정책 미기술) |
| SLO/용량 산정 가이드 | 없음 (M5 목표표는 계획 문서 안에만) |
| 알람 룰 예시 | 없음 |
| 실제 S3 검증 | 미완 (`todo.md` P2, MinIO만 검증됨) |

`docker-compose.yml`은 MinIO 부하 테스트용이며 서비스 자체를 배포하지 않는다.
`scripts/`도 부하 하네스 두 개뿐이다.

최소한 다음이 필요하다.
- 설정 레퍼런스 (`docs/CONFIGURATION.md`): knob별 의미, 기본값, 튜닝 방향, 상호 제약
  (P1-8의 `merge_max_input_bytes` vs `merge_max_memory_bytes` 같은 것)
- runbook: wedge 복구(`journal.wal.compact.state` 수동 삭제 포함), S3 장애 대응,
  디스크 풀 대응, 장비 교체 체크리스트
- 알람: `flush_errors` 증가율, `wal_backlog_bytes`, `merge_debt_parts`, `remote_healthy`,
  `pending_flush_bytes`

---

## 프로덕션 레디 게이트 (권장 순서)

### 게이트 1 — 데이터 안전성 (이것 없이 배포 금지)

- [ ] P0-1 WAL compaction wedge 수정 + 연속 compaction 테스트 + 각 크래시 지점 주입 테스트
- [ ] P0-1 복구 절차 문서화 (기존 wedge 상태에서 stale state 파일 제거)
- [ ] P0-2 ingest backpressure (memtable/WAL backlog 상한 → 429)
- [ ] P1-4 writer fencing (manifest epoch/lease + self-fence)
- [x] P1-6 타임스탬프 수용 윈도우
- [ ] P1-8 merge 상한 단위 불일치 수정 + 실패 시 그룹 분할 fallback

### 게이트 2 — 테넌시와 입력 방어

- [x] TLS 미지원을 아키텍처 결정으로 명문화 (`ARCHITECTURE.md` "전송 보안")
- [ ] P0-3 `X-Scope-OrgID` 추출 + 저장 경로 분할 축으로 도입
- [ ] P0-3 테넌트별 스로틀·quota (ingest rate, 카디널리티, 용량, 동시 쿼리) + 테넌트 라벨 metrics
- [ ] 기본 바인드를 신뢰 경계에 맞게 조정 (`0.0.0.0` → 명시적 설정 요구)
- [ ] P2-2 메타데이터 엔드포인트 리소스 가드
- [x] P2-3 snappy 신고 길이 검증 + body 상한 노출
- [x] P1-7 라벨/라인 크기 제한 (스트림 수 상한은 테넌시와 함께)

### 게이트 3 — 운영 가능성

- [ ] P1-10 시작 시 일시 장애 재시도 (crash loop 제거)
- [x] P2-6 `RUST_LOG` 반영, 기본 `info`
- [ ] P2-7 히스토그램 + 엔드포인트 라벨 (p95/p99 관측 가능)
- [ ] P2-8 stdin 아닌 abort 경로 + 오케스트레이터 배치 요구사항 문서화
- [x] P2-9 `file://` 프로덕션 오사용 경고 (opt-in 강제는 남음)
- [ ] P2-4 `retention_period` 기본값 결정 (무한 유지 시 그 이유를 문서화)
- [ ] P3 Dockerfile + 설정 레퍼런스 + runbook + 알람 룰

### 게이트 4 — 규모 검증

- [ ] P1-11 part 수 O(N) 경로 개선 (카탈로그 병렬 복원, manifest delta)
- [ ] P1-1 Tempo search 시간 프루닝
- [ ] P1-3 group commit 지연 구조 개선
- [ ] P1-5 memtable size O(1) 추적, flush clone 제거
- [ ] P1-9 eviction을 `spawn_blocking` + 인메모리 메타데이터로
- [ ] 실제 S3 + 목표 사양(4 vCPU / 16 GiB)에서 **최소 24시간** 지속 부하 (`todo.md` P2)
- [ ] part 10,000개 이상 상태에서 시작 시간·flush 지연·쿼리 계획 시간 측정

### 게이트 5 — 기능 완성도

- [ ] P1-2 OTLP 로그 (또는 문서 정정)
- [ ] P2-1 Loki API 공백 (특히 `tail`, `labels`/`series`의 시간 범위 반영)
- [ ] P2-5 중복 관측 가능성 → 이후 dedup 구현 (`todo.md` P2)
- [ ] `todo.md` P1의 LogQL 기능 보강

---

## 잘 되어 있는 부분

리뷰 중 특히 견고하다고 판단한 것들 — 앞으로 수정할 때 깨뜨리지 말아야 할 자산이다.

- **크래시 복구 불변량이 코드와 주석 양쪽에 명시적이다.** flush transaction, merge tombstone
  연쇄 복구, upload marker, phase 기반 compaction intent — 각 크래시 지점에서 무엇이 보장되는지
  주석이 이유까지 적고 있다. 이 수준은 드물다.
- **경로 안전성 검증이 일관되다.** `is_safe_path_component`, symlink 거부, canonical root 확인이
  캐시·manifest·transaction 경로 전반에 빠짐없이 들어가 있다.
- **로그/메트릭 쿼리 경로의 리소스 예산이 촘촘하다.** scan rows/bytes, materialized memory,
  concurrency semaphore, 타임아웃, cancellation flag가 모두 있고 테스트도 있다.
- **remote health의 epoch 기반 CAS**(`catalog.rs`의 `remote_state`)로 오래된 성공이 최신 실패를
  덮지 않게 한 것은 정확한 처리다.
- **flush 가시성 전환이 단일 write lock 안에 있어** part 등록과 memtable commit이 원자적이다.
- **M7 부하 검증이 스스로 블로커를 찾아냈고, 그것을 숨기지 않고 FAIL로 기록했다.**
  `docs/M7_LOAD_RESULTS.md`의 근본 원인 분석은 이 리뷰의 재현 결과와 정확히 일치한다.
- 211개 테스트가 전부 통과하며, 특히 저널·part·object_storage 경로의 크래시 주입 테스트가 실질적이다.

---

## 부록 — P0-1 재현 방법

`src/journal/tests.rs`에 아래를 추가하면 재현된다 (두 번째 레코드가 첫 번째보다 작아야 한다).

```rust
#[tokio::test]
async fn two_consecutive_compactions_wedge() {
    let harness = harness("two_compactions").await;
    push(&harness, make_push_req(&[("{app=\"a\"}", vec![("one", 100)])])).await;
    let first = harness.journal.checkpoint().await.unwrap();
    harness.memtable.commit_flush();
    harness.journal.compact_checkpoint(first.offset).await.unwrap();

    push(&harness, make_push_req(&[("{app=\"b\"}", vec![("2", 200)])])).await;
    let second = harness.journal.checkpoint().await.unwrap();
    harness.memtable.commit_flush();
    let result = harness.journal.compact_checkpoint(second.offset).await;
    assert!(result.is_ok(), "second compaction failed: {result:?}");
}
```

관측 결과:

```
first_offset=32 wal_after_first=0 second_offset=31
result=Err(Custom { kind: InvalidInput, error: "WAL compaction checkpoint moved backwards" })
wal_after_second=31
```

두 번째 레코드를 첫 번째와 **같은 크기**로 만들면 `offset == state.offset` 분기를 타서
`Ok`가 반환되지만 WAL이 잘리지 않는다 (조용한 no-op).
