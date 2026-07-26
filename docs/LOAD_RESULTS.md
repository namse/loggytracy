# 부하·측정 결과 (living)

측정한 것을 남기는 문서다. 채팅이나 터미널에만 있던 숫자는 사라지고, 사라진 숫자는 다음에 다시
재게 된다. 정책과 절차는 [`LOAD_VALIDATION.md`](LOAD_VALIDATION.md)에 있다.

**재현 가능한 것은 문서가 아니라 테스트로 둔다.** 아래 표의 "고정 위치"가 그 테스트 이름이며,
숫자만 여기 적는다. 테스트가 없는 항목은 부하 런이 필요한 것들이다.

머신: `Darwin arm64` (Apple Silicon), 8 논리 CPU, 16 GiB. **목표 사양(4 vCPU / 16 GiB)이 아니므로
절대 수치는 판정이 아니라 기록이다.**

---

## 1. group commit 지연 제거 (P1-3)

배치 루프가 채널이 비어 있어도 `max_batch_ms`를 소진하던 것을 고친 전후. Tier B, 45초,
3000 eps 제안.

| | 이전 (`782e7ff`) | 이후 (`e28f605`) |
|---|---|---|
| ack p50 | 208.8 ms | **5.9 ms** |
| ack p95 | 212.2 ms | **11.7 ms** |
| ack p99 | 214.4 ms | **37.1 ms** |

ack 지연이 저장 백엔드가 아니라 타이머에 지배당하고 있었다는 증거: `file://`이든 MinIO든,
지연 주입이 있든 없든 이전 값은 전부 ~250 ms에 붙어 있었다.

고정 위치: `journal::tests::sequential_appends_do_not_wait_out_a_batch_timer`
(20회 순차 append가 1.5초 안에 끝나야 한다 — 옛 기본값이면 4초였다).

## 2. 테넌트 파편화 비용 (N3)

동일한 5,000행, 동일한 `row_group_size=8192`, **테넌트 수만 다름**.

| | row group | `data.parquet` |
|---|---|---|
| 테넌트 1개 | 1 | 28,029 B |
| 테넌트 500개 | 500 | **691,119 B** |

**24.7배.** row group 하나당 약 1,330 B의 구조적 오버헤드이고, 행당으로는 5.6 B → 133 B다.
row group은 테넌트 경계에서 끊기므로 **테넌트 수가 row group 수의 하한**이고, Parquet은 row
group마다 컬럼 메타데이터를, 이 엔진은 row group마다 bloom 필터를 싣는다.

목표 워크로드가 "작은 테넌트 다수"라는 점에서 이 숫자는 설계 판단에 직접 들어간다. 완화 방향은
`todo.md`의 part 사이드카 통합과 Parquet range read 항목에 걸려 있다.

고정 위치: `part::tests::tenant_breadth_sets_the_row_group_floor_and_what_that_costs`
(비율은 zstd에 달려 있으므로 임계로 못 박지 않고 관계만 고정한다).

## 3. backpressure가 상한에서 버틴다

memtable 상한을 **8 MiB로 낮추고** 페이싱 없이 밀었다. 25만 이벤트, 500 테넌트, 실 S3에 가까운
지연 주입(20 + uniform(0,180) ms), 오류 주입 0.2%.

| | 값 |
|---|---|
| 판정 | **PASS** |
| 통과한 이벤트 | 250,000 |
| 429로 거절 | **512,546** |
| 실제 오류 | **0** (서버 `ingest_errors_total` 미증가) |
| ack p50 / p95 / p99 | 5.9 / 12.1 / 16.2 ms |
| memtable 종료값 | 8.19 MB (상한 8 MiB 바로 아래) |
| WAL backlog | 8.36 MB (유계) |
| RSS 최대 | 4.95 MB |
| flush 성공 / 주입오류 회복 | 64 / 2 |

**읽는 법:** 유입이 flush 능력을 크게 넘을 때 시스템은 무한히 버퍼링하지 않고 **상한에 붙어
거절한다.** 429가 50만 회를 넘는 것은 고장이 아니라 그 지점에서 backpressure가 유일하게 옳은
동작을 했다는 뜻이다. ack 지연이 이 상황에서도 12 ms대라는 점이 함께 중요하다 — 받아들인 요청은
빠르고, 못 받을 요청은 즉시 거절한다.

고정 위치(로직 부분): `ingest::tests::push_is_refused_once_the_memtable_is_over_its_limit`,
`ingest::tests::push_is_refused_once_the_wal_backlog_is_over_its_limit` — 둘 다 **걸리는 것과
풀리는 것**을 함께 검사한다. backpressure가 래치가 되면 한 번의 버스트가 인스턴스를 영구히
서비스에서 빼므로, 이것이 무한 증가보다 나쁘다.

### 이 런이 드러낸 하네스 결함

첫 실행은 `error_rate 0.995`로 numeric FAIL이었다. **하네스가 429를 오류로 세고 있었다.**
설계대로 방어한 런이 99.5% 오류율로 보고되면 재앙처럼 읽힌다 — 실제로는 재앙을 막은 기능이다.
429를 `push_throttled`로 분리하고 오류율에서 제외했다(`e925334` 이후).

## 4. flush와 part 수의 관계

merge를 실제로 끈 상태(`MERGE_INTERVAL=3600s`, `RETENTION_PERIOD=off`)에서 **12회 flush → part
12개.** 예상대로다.

이 측정을 한 이유는 앞선 런에서 "33회 flush에 part 3개"가 관측돼 엔진 버그를 의심했기 때문인데,
원인은 `scripts/run_load_local.sh`가 호출자의 `MERGE_INTERVAL`을 조건 없이 덮어써서 실제로는
8초로 돌고 있었던 것이다. merge가 제 일을 하고 있었다. 스크립트는 고쳤다(`ea6b0b3` 부근).

## 5. eviction → restore

기본 구성으로는 관측되지 않는다. merge가 최근 part로 통합하고 그 결과물은 항상 로컬에 있으며,
retention이 옛 데이터를 지워 프로브가 빈 범위를 질의한다. 둘 다 정상 동작이다.

관측 구성과 결과:

```
MERGE_INTERVAL=3600s  RETENTION_PERIOD=off  CACHE_MAX_BYTES=524288
LOAD_RESTORE_LOOKBACK_SECONDS=40
```

| | 값 |
|---|---|
| `restore_observed` | **true** |
| 축출 | 111회 |
| part 수 | 66 |
| 복원 오류 | 0 |
| 복원 지연 p50 / p95 / p99 | 31 / 749 / 1,626 ms |

**남은 한계:** 프로브는 "복원해서 읽었다"와 "아무것도 매칭되지 않았다"를 구분하지 못한다 —
둘 다 200이다. 위 판정은 서버 카운터로 확인한 것이라 유효하지만, 프로브가 읽은 행 수를
확인하도록 만드는 것이 낫다.

---

## 아직 측정하지 않은 것

정직하게 남긴다.

- **장시간 누수.** 처리량이 아니라 "오래 살아있음"에 반응하는 것(fd 누수, 메모리 단편화)은
  위 런들이 답하지 않는다. 이것만 긴 런이 필요하고, 그래서 **가끔** 돌릴 항목이다.
- **part 수가 수만 개일 때의 시작 시간·쿼리 계획 시간(P1-11).** merge가 정상 동작하면 part 수는
  유계로 유지되므로, 재려면 merge를 끈 별도 구성이 필요하다. 4번 런이 그 방향의 첫 걸음이다.
- **오브젝트 스토어 연산 횟수.** 비용 예측의 대리 변수인데 계측이 없다.
