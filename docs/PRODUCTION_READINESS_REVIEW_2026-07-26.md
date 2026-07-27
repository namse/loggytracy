# 프로덕션 레디 리뷰 (2026-07-26, fresh context)

리뷰 대상 리비전: `32dd7b6` (테넌시 1단계 + retention push 완료 시점, working tree clean)
리뷰 범위: `src/` 전체 (28,300 LOC), `docs/`, 배포 자산
검증: `cargo test` 274개 전부 통과, `cargo clippy --all-targets` 경고 0

직전 리뷰는 [`PRODUCTION_READINESS_REVIEW.md`](PRODUCTION_READINESS_REVIEW.md) (리비전 `56afbbe`)이다.
이 문서는 그 이후 들어온 테넌시·retention 코드를 포함해 다시 처음부터 읽은 결과이며, 직전 리뷰의
항목 번호(P0-1 등)를 그대로 이어 쓰고 새로 발견한 것에는 `N` 접두사를 붙였다.

> **갱신(수정 후):** 아래 진단은 리뷰 시점(`32dd7b6`)의 것이다. 게이트 1의 P0-1·P0-2·N1은
> 이 리뷰 직후 수정했다. 각 항목 제목과 「수정 반영 이력」절을 참고할 것.

## 판정

**리뷰 시점 기준 프로덕션 투입 불가.** 직전 리뷰가 게이트 1(데이터 안전성)으로 지정한 P0-1과 P0-2가
그대로 열려 있고, 그 사이 들어온 테넌시/retention 코드가 P1-8(merge 상한 단위 불일치)과 맞물려
**테넌트 삭제의 물리적 보장까지 깨뜨리고 있다.**

직전 리뷰 대비 달라진 것:

| 항목 | 직전 | 현재 |
|---|---|---|
| P0-1 WAL compaction wedge | 열림 | **열림** (이번 리뷰에서 재현) |
| P0-2 ingest backpressure | 열림 | **열림** (429 경로가 코드에 없음) |
| P0-3 멀티테넌시 미구현 | 열림 | **식별·격리·retention은 닫힘**, 스로틀/quota는 열림 |
| P1-1 Tempo search 전량 복원 | 열림 | 부분 개선 (테넌트 범위로 축소, 시간 프루닝은 없음) |
| P1-4 writer fencing | 열림 | 열림 |
| P1-8 merge 상한 단위 불일치 | 열림 | **열림 + 심각도 상승** (N1 참조) |

## 이번 리뷰에서 재현한 것

P0-1은 코드 독해가 아니라 실행으로 확인했다. `src/journal/tests.rs`에 연속 compaction 테스트를
임시로 추가해 관측한 뒤 원복했다.

```
first_offset=57 wal_after_first=0 second_offset=49 wal_after_second=49
result=Err(InvalidInput, "WAL compaction checkpoint moved backwards")
```

---

## 게이트 1 — 배포 금지 조건 (열림)

### P0-1. WAL compaction wedge — 이후 **수정됨**

- 위치: `src/journal/compaction.rs`, `src/journal/replay.rs:37`, wedge 전파는 `src/flush.rs:51-84`

`compact_wal`은 마지막에 `write_compaction_state(&state_path, &state, 2)`로 상태 파일을 남기고
**삭제하지 않는다.** compaction은 WAL을 잘라내고 checkpoint를 0으로 리셋하므로 다음 offset은 새
좌표계에 사는데, 다음 호출은 그 offset을 옛 좌표계의 stale offset과 비교한다.

| 다음 offset | 결과 |
|---|---|
| `< state.offset` | `Err("WAL compaction checkpoint moved backwards")` → 영구 wedge |
| `== state.offset` | 조용한 no-op, WAL이 잘리지 않음 |
| `> state.offset` | 우연히 정상 |

`replay.rs:37`의 `if state.phase != 1 { return Ok(()) }` 때문에 재시작으로도 복구되지 않는다.
wedge 전파 경로도 확인했다: `flush.rs`의 `pending_checkpoint`가 영원히 `Some`으로 남아 매 tick
`continue`하므로 **새 flush 진입 자체가 차단**되고, 그동안 ingest는 계속 `204`를 반환한다.

### P0-2. ingest backpressure 부재 — 이후 **수정됨**

- 위치: `src/ingest.rs:114` (`push_inner`), `src/trace_ingest.rs`, `src/config.rs`

`rg "429|TOO_MANY_REQUESTS"` 결과 코드 전체에 429 경로가 없다. `push_inner`는 draining·body
크기·라벨·라인·타임스탬프만 검사하고 memtable/WAL backlog는 보지 않는다. 대응 knob도 없다.
P0-1과 결합하면 "장애 → 확정 OOM"이 그대로 성립한다.

### P1-4. 단일 writer 강제 장치 없음 (열림)

`src/object_storage/catalog.rs`에 lease/fencing token이 없다. 같은 prefix로 두 프로세스를 띄우면
둘 다 정상 동작한다고 믿고 쓴다. M6 장비 교체에서 구 인스턴스가 완전히 죽기 전에 신 인스턴스가
뜨면 정확히 이 상태가 된다.

---

## N — 이번 리뷰에서 새로 발견한 것

### N1. 테넌트 삭제(`retention: "0"`)가 물리적 삭제를 보장하지 않는다

두 경로가 동시에 성립한다.

**(a) 큰 part는 영구히 rewrite되지 않는다.**
`src/merge/scheduler.rs:150-160`은 retention-only 그룹의 read 실패를 `retention_rewrite_skipped`로
세고 `continue`한다. 주석은 "입력이 고정이니 다음 tick에도 안 맞는다, 놓친 optimization일 뿐"이라고
정당화하는데, **zero-retention 경로에서 그것은 optimization이 아니라 삭제 자체**다.

기본값으로 계산하면:

| 값 | 기본 |
|---|---|
| `merge_max_part_rows` | 4,000,000 |
| `merge_max_memory_bytes` | 1 GiB |
| 행당 예산 | 약 268 B (`size_of::<Row>()` 포함) |

짧은 라인이 아닌 이상 full-size part는 rewrite가 구조적으로 불가능하고, 그 안의 삭제된 테넌트
행은 디스크와 S3에 영구히 남는다. 직전 리뷰는 P1-8을 merge 성능 문제로 분류했으나, 테넌트 삭제가
생긴 지금은 **삭제 요청 대응 불가**라는 성격이 함께 붙는다.

**(b) 삭제가 런타임 플래그 하나에 의존한다.**
`LOGGYTRACY_TENANT_POLICY_TOKEN`이 빠지면 `TenantPolicy::load`(`tenant_policy.rs:419`)가
`disabled()`를 반환한다. 그러면 `query_floor_ns`가 전부 `None`이 되어 **rewrite되지 않고 숨겨져
있던 삭제 데이터가 전부 다시 조회된다.** 동시에 admin 라우트도 사라져(`router.rs:35`) 운영자가
알아챌 표면이 없다. env 하나 빠뜨린 재시작이 곧 데이터 부활이다.

**수정 방향**
- rewrite 실패 시 그룹을 쪼개 재시도하고, 단일 part는 row group 범위로 나눠 여러 출력 part로
  재작성한다. zero-retention 경로의 실패는 `retention_rewrite_skipped`가 아니라 오류로 승격한다.
- 저장된 정책이 하나라도 있는데 토큰이 없으면 부팅을 실패시킨다.

### N2. 테넌트 allowlist가 없다

`TenantId::parse`(`src/tenant.rs:18`)는 `[a-zA-Z0-9_-]{1,64}`만 본다. 허용 목록 검증이 없어
헤더를 넣을 수 있는 누구나 무제한 테넌트를 만들 수 있다. `todo.md`는 이 항목을 완료로 표시하고
있으나 코드에는 없다. 결과는 N3·N4로 증폭된다.

### N3. row group이 테넌트 경계로 강제 분할된다

`src/part/format.rs:210` `row_group_bounds`는 테넌트가 바뀔 때마다 row group을 끊는다. 정합성은
맞지만 **테넌트 수가 row group 수의 하한**이 된다. 작은 테넌트가 많은 목표 워크로드에서 flush
하나에 500 테넌트가 섞이면 5행짜리 row group 500개가 되고, parquet 컬럼 청크 메타데이터와 row
group당 bloom 필터가 테넌트 수에 비례한다. 압축률도 무너진다. `flush_max_bytes` 1 MiB 기본값과
함께 봐야 하는 축이다.

### N4. `/metrics`가 스크레이프마다 전체 part를 순회한다

`tenant_policy_gauges`(`src/query/handlers.rs:587`)가 모든 part의 테넌트 세그먼트를 훑고,
같은 핸들러의 `merge_debt_part_count`는 `select_groups` → `estimated_part_bytes` → part마다
`fs::metadata`를 호출한다. 무인증 엔드포인트에서 O(parts × tenants) 작업이며, 직전 리뷰의 P2-7
지적보다 오히려 무거워졌다.

### N5. part 온디스크 포맷에 버전 필드가 없고 업그레이드 경로가 비대칭이다

`MetaFile`(`src/part/metadata.rs:99`)에 `tenants: Vec<TenantSegment>`가 `#[serde(default)]` 없이
추가됐고 `version` 필드도 없다. 기존 배포의 part는 `meta.json` 역직렬화가 실패해
`startup.rs:132`에서 패닉한다.

반면 WAL은 `replay.rs:145-153`에서 pre-tenancy 레코드를 default 테넌트로 명시 처리한다.
**저널은 무손실 업그레이드를 설계했는데 part는 아예 못 읽는다.** 지금은 데이터가 없어 문제되지
않지만, 포맷 버전 필드가 없는 한 앞으로의 모든 스키마 변경이 같은 벽에 부딪힌다.

### N6. Tempo 메타데이터 경로는 개선됐으나 시간 프루닝이 없다

`pin_all_trace_parts`가 `tenant_part_ids(tenant)`로 바뀌어 테넌트 범위로는 좁혀졌다(P1-1 부분
개선). 그러나 `search`는 start/end를 계산해 놓고 `scan_trace_spans(..., None, ...)`로 넘겨
**스캔 후에 필터**하고(`tempo/handlers.rs:113-130`), `search_tags`/`search_tag_values`는 시간
파라미터 자체가 없다. Grafana 태그 드롭다운 한 번이 해당 테넌트 트레이스 전량 복원인 것은 그대로다.

---

## 직전 리뷰에서 여전히 열려 있는 것

확인만 하고 상세는 직전 문서를 참조한다.

| 항목 | 상태 |
|---|---|
| P1-2 OTLP 로그 미구현 | 열림 (`startup.rs:372`에 등록된 gRPC 서비스는 trace 하나) |
| P1-3 group commit이 batch 타이머 소진 | 열림 |
| P1-5 memtable O(rows) 크기 계산 + flush deep clone | 크기 계산은 **수정됨**(P0-2와 함께), deep clone은 열림 |
| P1-9 eviction이 write lock 잡고 동기 디렉터리 순회 | 열림 |
| P1-10 시작 시 오브젝트 스토어 오류가 패닉 | 열림 (`startup.rs`에 `panic!` 8곳) |
| P1-11 시작/flush 비용이 part 수에 선형 | 열림 |
| P2-1 Loki/Tempo API 공백 | 열림 |
| P2-2 메타데이터 엔드포인트 리소스 가드 | 열림 (`labels`/`label_values`/`series`/`index_stats`가 `start`/`end`도 무시) |
| P2-5 크래시 후 중복 관측 불가 | 열림 |
| P2-7 히스토그램·라벨 없는 `/metrics` | 열림 (N4로 악화. WAL backlog의 스크레이프당 `stat`은 P0-2와 함께 제거) |
| P2-8 stdin abort가 컨테이너에서 무효 | 열림 |
| P3 배포 자산 | 열림 (Dockerfile·설정 레퍼런스·runbook 전부 없음) |
| P2 실 S3 검증 | **범위 밖으로 확정** — 로컬 MinIO가 상한 ([`LOAD_VALIDATION.md`](LOAD_VALIDATION.md)) |

---

## 잘 된 부분 (이번에 새로 들어온 코드 기준)

- **retention push 설계.** "저장 후 ack"(`tenant_policy.rs:497-513`), 부팅 시 로드 실패는 치명적,
  *미지 테넌트* ≠ *infinite* 구분, `write_lock` 안에서 `updated_at`을 찍어 push age 역행을 막은 것까지
  이유가 코드에 남아 있다. 폴링과 `reqwest` 의존성을 없애 나가는 호출이 오브젝트 스토어뿐인 것도 좋다.
- **admin 인증.** 토큰 미설정 시 라우트를 아예 mount하지 않고, `secret_matches`는 상수 시간 비교,
  4 KiB body 상한, `push_rejected`와 `admin_unauthorized`를 분리해 "control plane이 고장났나"와
  "누가 두드리나"를 다른 질문으로 센 판단이 정확하다.
- **테넌트 격리가 읽기 경로 전반에 빠짐없이 적용됐다.** `query_floor_ns`가 log/metric/labels/
  label_values/series/index_stats/tempo 전 핸들러에 걸려 있고, 공유 part에서 테넌트 경계로 row
  group을 끊어 격리를 인덱스 수준에서 보장한다.
- **retention이 자기 part를 직접 쓰지 않고** merge의 트랜잭션·tombstone·manifest CAS를 재사용하도록
  한 결정은 크래시 안전성 표면을 늘리지 않은 좋은 선택이다.
- 로그 삭제 후 **trace 삭제 실패를 기다리지 않고 즉시 unregister**하도록 고친 것
  (`retention.rs:255-262`)은 정확한 수정이다.

---

## 수정 반영 이력

이 리뷰 직후 게이트 1 항목을 한 배치로 처리했다. 테스트 285개 통과, clippy 경고 0.

| 항목 | 상태 | 요지 |
|---|---|---|
| P0-1 WAL compaction wedge | 수정됨 | intent 레코드를 성공 직후 durable 제거. phase 2는 "레코드 부재"로 표현하고, 남아 있는 phase-2 레코드는 완료로 간주해 제거 — 이미 wedge된 인스턴스도 업그레이드만으로 자동 복구된다 |
| P0-2 ingest backpressure | 수정됨 | memtable·WAL backlog를 O(1)로 추적하고, 상한 초과 시 journal append 이전에 `429` + `Retry-After` (OTLP는 `RESOURCE_EXHAUSTED`) |
| N1(a) merge 분할 fallback | 수정됨 | 그룹은 절반씩, 단일 part는 row group 윈도로 나눠 재작성. 테넌트 삭제가 큰 part에서 영구 skip되지 않는다 |
| N1(b) 정책 토큰 없는 부팅 | 수정됨 | 저장된 정책이 하나라도 있는데 토큰이 없으면 부팅 실패 |
| N5 포맷 버전 필드 | 수정됨 | part·trace part `meta.json`에 `version`. 체크섬 검증 **이전**에 확인하므로 포맷 차이가 손상으로 보이지 않는다 |
| P1-8 merge 상한 단위 | 수정됨 | part meta에 `materialized_bytes`. 그룹 선택과 읽기 예산이 같은 단위를 쓰고, `validate`가 두 상한의 순서를 강제한다 |
| N4 `/metrics` O(parts) | 수정됨 | merge debt는 merge 워커가, unknown tenant는 retention 워커가 발행. 스크레이프는 읽기만 한다 |
| N2 테넌트 allowlist | 수정됨 | `LOGGYTRACY_ALLOWED_TENANTS`. 목록 밖 테넌트는 403 |
| N6 Tempo 시간 프루닝 | **부분 수정** | `search_tags`·`search_tag_values`가 `start`/`end`를 받고, 그 범위가 pin 집합과 row group 선택까지 내려간다. `search`는 손대지 않았다 — 아래 참고 |
| P2-2 메타데이터 가드 | 수정됨 | semaphore·타임아웃·`start`/`end`·`match[]` 개수 상한 |
| P1-4 writer fencing | 수정됨 | manifest의 `writer_epoch`. 시작 시 claim, 모든 CAS에서 검증, fence 시 self-fence 후 비정상 종료 |

### P0-1 — 수정 내용

`compact_wal`은 성공 경로 마지막에서 intent 레코드를 지우고 부모 디렉터리를 fsync한다. 이제
정상 상태에서는 레코드가 존재하지 않으므로, 다음 compaction이 좌표계가 다른 offset끼리 비교하는
상황 자체가 사라진다. 남아 있는 레코드는 두 종류뿐이고 각각 뜻이 하나다.

| 레코드 | 뜻 | 처리 |
|---|---|---|
| phase 1 + tmp 존재 | rename 전 크래시 | 옛 checkpoint 복원 후 재시도 (기존과 동일) |
| phase 1 + tmp 없음 | rename 후 크래시 | 완료로 간주, 레코드 제거 |
| phase 2 | 구 빌드의 잔존물 | 완료로 간주, 레코드 제거 |

`replay.rs`도 같은 규칙을 따르므로 재시작 경로에서도 복구된다. 테스트는 연속 compaction
(축소·동일·증가 세 케이스), 구 빌드 phase-2 잔존물의 compaction·replay 양쪽 복구, intent 제거
실패 후 호출자 재시도까지 덮는다. 크래시 주입은 WAL 경로별로 armed되도록 바꿨다 — 프로세스 전역
플래그는 병렬 테스트에서 armed하지 않은 쪽이 먼저 소비한다.

### P0-2 — 수정 내용

`MemTable`/`TraceMemTable`이 바이트 총량을 원자 카운터로 유지한다(P1-5의 절반). 두 버퍼가 병합될
때 스트림 식별자가 이중 계상되므로 `merge_snapshot`이 중복분을 돌려주고 카운터에서 빼며, 그 결과
카운터는 전수 순회 값과 **정확히** 일치한다(테스트로 고정). `Journal`은 `wal_bytes`/
`checkpoint_bytes`를 들고 있어 backlog도 O(1)이다 — `/metrics`도 더 이상 스크레이프마다 `stat` +
checkpoint 파일 읽기를 하지 않는다.

`IngestGate`가 두 프로토콜의 단일 판정 지점이다. 한쪽만 막으면 초과분이 다른 쪽으로 옮겨갈 뿐이다.
새 knob은 `LOGGYTRACY_MAX_MEMTABLE_BYTES`(기본 256 MiB), `LOGGYTRACY_MAX_WAL_BACKLOG_BYTES`
(기본 1 GiB), `LOGGYTRACY_BACKPRESSURE_RETRY_AFTER`(기본 1s)이며 앞의 둘은 `off`로 끌 수 있다.
`config.validate`는 memtable 상한이 `flush_max_bytes`보다 낮은 조합을 거절한다 — flush에게
옮기라고 시키지도 않은 데이터를 이유로 쓰기를 거절하게 된다.

새 metric: `loggytracy_ingest_throttled_total`, `loggytracy_memtable_buffered_bytes`.

### N1(a) — 수정 내용

`rewrite_group`이 읽기·필터·쓰기를 한 blocking 태스크 안에서 번갈아 수행한다. 배치를 모아 두면
분할이 아끼려던 메모리를 그대로 쓰게 되기 때문이다. 상한을 넘으면 part가 여럿인 그룹은 절반으로
쪼개 재귀하고, part 하나짜리는 `PartReader::read_rows_in_row_groups`로 row group 윈도를 순회한다.
윈도 크기는 part의 실제 평균 행 폭에서 계산한다. 출력 part가 몇 개가 되든 전부 같은 merge
tombstone(`old_dirs`)을 달고 나가므로 커밋은 여전히 한 트랜잭션이다. 중간 실패 시 이미 쓴 출력은
그 자리에서 제거한다.

`retention_rewrite_skipped`는 이제 "분할해도 안 되는 경우"만 센다 — 실질적으로 단일 row group이
예산을 넘는 설정 문제다. 아울러 retention 전용 그룹은 `meta.json`만 보고 이미 회수된 그룹을
읽기 전에 걸러낸다.

### N5·P1-8 — 수정 내용

`meta.json`의 `version`은 체크섬 검증 **이전**에 확인한다. 체크섬은 구조체 위에서 계산되므로
양쪽이 구조체가 무엇인지 합의한 뒤에야 의미가 있고, 순서를 반대로 하면 포맷 변경이 체크섬
불일치로 보인다 — 그건 버전 차이가 아니라 디스크 고장처럼 읽힌다. manifest는 이미
`format_version`을 갖고 있었다.

P1-8은 part meta에 `materialized_bytes`(읽었을 때 실제로 차지하는 메모리)를 기록해 해결했다.
`Row::materialized_bytes` 하나로 계산을 모아 그룹 선택과 읽기 예산이 어긋날 수 없게 했고,
`estimated_part_bytes`의 `fs::metadata`가 사라져 N4도 함께 가벼워졌다.

### N2·P2-2 — 수정 내용

allowlist 밖 테넌트는 **403**이다. 400이 아닌 이유는 요청 자체는 정상이고 클라이언트가 고칠 것이
없기 때문이다. 목록을 켜면서 기본 테넌트를 빼놓으면 헤더 없는 요청마다 목록 밖 테넌트가 생기므로
`validate`가 거절한다.

메타데이터 4종은 `MetadataGuard`를 획득한다. retention floor는 `start_ns`에 접어 넣어
"클라이언트가 요청한 범위"와 "테넌트가 아직 볼 자격이 있는 범위"를 한 경계(`MetadataWindow`)로
표현한다. `start`가 없으면 전체 히스토리가 아니라 `max_query_range`만큼만 거슬러 올라간다 —
무한 기본값이 애초에 전체 part를 읽게 만든 원인이다. 빈 범위는 오류가 아니라 빈 응답이다.

### P1-4 — 수정 내용

`writer_epoch`를 별도 객체가 아니라 manifest 안에 둔 이유는 검사 비용이 0이기 때문이다. 모든
쓰기는 이미 자기가 대체할 manifest를 읽는다. 시작 시 log·trace 양쪽에 같은 번호를 새기고
(한쪽만 claim하면 트레이스 쓰기가 fencing되지 않는다), 이후 모든 CAS가 읽어 온 epoch를 검증한다.

fence 감지는 `ObjectStorage`가 `ShutdownState`에 직접 알린다. 워커마다 fencing이 무엇인지 알
필요 없이 flush·merge·retention·force-flush가 동일하게 반응한다.

**self-fence 동작은 이 작업에서 내린 결정이다.** fence를 만난 force-flush는 재시도를 멈춘다.
다른 모든 force-flush 실패는 일시적이라 무한 재시도가 옳지만 이것은 아니다 — 계속 돌면
오케스트레이터가 죽일 때까지 프로세스가 살아 있고, 그 뒤 파드가 다른 노드에 스케줄되면 미flush
데이터의 유일한 사본을 가진 디스크가 버려진다. M6가 막으려던 손실이 정확히 그 경로로 발생한다.
종료 코드 1, 데이터는 WAL에 남으며 로그가 디스크 보존을 명시한다.

**운영상의 함의:** 새 인스턴스는 시작 즉시 epoch를 claim한다. 따라서 M6 절차의 "구 인스턴스를
완전히 drain한 뒤 신 인스턴스를 띄운다"가 이제 **강제**된다. 순서를 어기면 구 인스턴스는 조용히
망가지는 대신 즉시 멈추고 비정상 종료한다.

나머지는 `todo.md`에 남긴다.

---

## 프로덕션 레디 게이트 (갱신)

### 게이트 1 — 데이터 안전성

- [x] P0-1 WAL compaction wedge 수정 + 연속 compaction·크래시 주입 테스트
- [x] P0-2 ingest backpressure (memtable/WAL backlog 상한 → 429)
- [x] N1(a) merge 메모리 초과 시 분할 fallback (테넌트 삭제 보장)
- [x] N1(b) 저장된 정책이 있는데 토큰이 없으면 부팅 실패
- [x] P1-4 writer fencing (manifest epoch + self-fence)
- [x] P1-8 `merge_max_input_bytes` vs `merge_max_memory_bytes` 단위 통일

### 게이트 2 — 테넌시 마무리

- [x] N2 테넌트 allowlist
- [ ] 테넌트별 스로틀·quota·`max_streams_per_user`, 테넌트 라벨 metrics
- [x] P2-2 메타데이터 엔드포인트 리소스 가드 + `start`/`end` 반영
- [x] N4 `/metrics` O(parts) 제거
- [ ] 기본 바인드를 신뢰 경계에 맞게 조정

### 게이트 3 — 운영 가능성

- [x] N5 part 포맷 버전 필드
- [ ] P1-10 시작 시 일시 장애 재시도 (crash loop 제거)
- [ ] P2-7 히스토그램 + 엔드포인트 라벨
- [ ] P2-8 stdin 아닌 abort 경로
- [ ] P3 Dockerfile + 설정 레퍼런스 + runbook + 알람 룰

### 게이트 4 — 규모 검증

- [ ] N3 테넌트 다수 환경의 row group 파편화 측정
- [x] N6 Tempo 태그 엔드포인트 시간 프루닝
- [ ] **N6 잔여: `search`의 시간 프루닝은 의미 결정이 먼저다.** 지금 `search`는 트레이스의
      *가장 이른* 스팬이 창 안에 있을 때만 반환한다. 그 스팬이 창이 닿지 않는 row group에
      있을 수 있으므로, 프루닝은 비용만이 아니라 **어떤 트레이스가 나오는지를 바꾼다**.
      경계에 걸친 트레이스에서 `startTimeUnixNano`와 `durationMs`도 달라진다.
      Tempo 본래 의미는 "스팬 하나라도 창에 겹치면 반환"이라 지금 구현이 더 좁다.
      의미를 Tempo에 맞추기로 하면 프루닝은 따라온다 — 순서가 그 반대다
- [ ] P1-11 part 수 O(N) 경로 개선
- [ ] P1-5 memtable flush deep clone 제거
- [ ] P1-9 eviction을 `spawn_blocking` + 인메모리 메타데이터로
- [ ] Tier D 지속·규모 런 (2시간 이상, part 10,000개 이상, 테넌트 500개 이상) —
      **실 S3 검증은 범위 밖으로 확정**되었다. 근거와 남은 위험은
      [`LOAD_VALIDATION.md`](LOAD_VALIDATION.md)

### 게이트 5 — 기능 완성도

- [ ] P1-2 OTLP 로그 (또는 문서 정정)
- [ ] P2-1 Loki API 공백
- [ ] P2-5 중복 관측 가능성 → dedup
- [ ] `todo.md` P1의 LogQL 기능 보강
