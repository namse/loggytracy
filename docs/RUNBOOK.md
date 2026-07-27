# 운영 runbook

설정값의 의미는 [`CONFIGURATION.md`](CONFIGURATION.md), 설계 근거는
[`ARCHITECTURE.md`](ARCHITECTURE.md)에 있다. 이 문서는 **무언가 잘못됐을 때 무엇을 보고 무엇을
하는가**만 다룬다.

---

## 배포의 전제 — 이것부터

이 엔진은 **싱글 머신, 단일 writer**다. 그 전제가 깨지면 데이터가 상한다.

1. **디스크가 파드를 따라다녀야 한다.** `LOGGYTRACY_DATA_DIR`의 WAL은 마지막 flush 이후
   ack된 데이터의 **유일한 사본**이다. StatefulSet + 고정 PV여야 하고, 파드가 다른 노드로
   재스케줄될 때 볼륨이 따라가야 한다. Deployment + emptyDir는 데이터 손실 구성이다.
2. **`terminationGracePeriodSeconds`를 크게 잡아야 한다.** 종료 시 force-flush는 하드
   타임아웃 없이 재시도한다. 오케스트레이터가 30초 뒤 SIGKILL하면 미flush 데이터가 WAL에만
   남고, 그 뒤 다른 노드에 스케줄되면 그 디스크가 버려진다. **최소 10분, 가능하면 더.**
3. **레플리카는 1이다.** 2 이상으로 올리면 두 번째 인스턴스가 writer epoch를 가져가고
   첫 번째가 fence되어 죽는다. 그렇게 설계했지만, 애초에 그런 구성을 만들면 안 된다.
4. **리스닝 주소는 신뢰 경계 안에.** TLS도 인증도 이 프로세스 밖의 일이고, `X-Scope-OrgID`를
   증명 없이 신뢰한다.

## 무엇을 알람으로 걸 것인가

| 신호 | 조건 | 뜻 |
|---|---|---|
| `loggytracy_ingest_throttled_total` | 증가 중 | 429를 내보내는 중. flush가 ingest를 못 따라간다 |
| `loggytracy_wal_backlog_bytes` | 상승 추세 | 위와 같은 원인, 더 이른 신호 |
| `loggytracy_flush_errors_total` | 증가하는데 `flush_success_total`은 정체 | **flush 정지.** 가장 위험한 상태 |
| `loggytracy_remote_healthy` | 0 지속 | 오브젝트 스토어 도달 불가 |
| `loggytracy_merge_debt_parts` | 상승 추세 | merge가 못 따라간다. 쿼리 계획 비용이 오른다 |
| `loggytracy_retention_rewrite_skipped_total` | 증가 | 너무 커서 재작성 못 하는 part가 있다. **테넌트 삭제가 완료되지 않는다** |
| `loggytracy_tenant_policy_unknown_tenants` | 0보다 큼 | control plane이 모르는 테넌트가 데이터를 쌓고 있다 |
| `loggytracy_pending_flush_bytes` | draining 중 0으로 안 감 | 종료가 durability에 도달하지 못하고 있다 |
| `loggytracy_part_sidecar_resident_bytes` | RSS 예산 대비 상승 | 사이드카는 캐시 축출 대상이 아니다. part 수에 선형인 상주 메모리 |
| `loggytracy_part_tenant_segments` | `part_count × 테넌트 수`에 근접 | 테넌트마다 거의 모든 part에 흩어져 있다. 공유 part의 고정비를 최대로 내는 상태 |

`part_tenant_segments`는 (테넌트, part) 쌍의 수다. `part_count`로 나누면 part 하나의
평균 테넌트 폭이고, 테넌트 수로 나누면 **테넌트 하나가 몇 개 part에 흩어져 있는가**다.
쌍마다 row group 하나·bloom 둘·메타데이터 세그먼트 하나가 나가므로, 사용량이 거의 없는
테넌트의 비용은 자기 유입량이 아니라 이 값이 정한다.

`/ready`는 flush·merge·retention·OTLP·오브젝트 스토어·로컬 캐시가 **각각 독립적으로** 내린다.
503 본문에 어느 것이 문제인지 적혀 있다.

---

## 증상별 대응

### `/ready`가 503에서 안 돌아온다

```
curl -s localhost:3100/ready          # 어느 컴포넌트인지 본문에 나온다
curl -s localhost:3100/metrics | grep _errors_total
```

무엇이 늘고 있는지에 따라 아래로 간다.

### flush가 멈췄다 (`flush_errors`만 증가, `flush_success` 정체)

WAL backlog와 memtable이 계속 자라고, 상한을 넘으면 429가 나가기 시작한다. 데이터는 아직
안전하다 — WAL에 있다.

1. `remote_healthy`가 0이면 오브젝트 스토어 문제다. 아래 항목으로.
2. 로그에 `fenced by a newer writer`가 있으면 **다른 인스턴스가 prefix를 가져갔다.**
   이 프로세스는 곧 종료된다. 두 인스턴스를 띄운 원인을 먼저 찾는다. 이 디스크는 미flush
   데이터를 갖고 있으므로 **버리지 않는다.**
3. 디스크가 찼는지 본다. WAL은 flush가 성공해야 잘린다.

### 오브젝트 스토어에 도달하지 못한다

엔진은 무한 재시도하고 `/ready`를 503으로 둔다. 자동 복구되므로 **기다리는 것이 기본 대응**이다.
ingest는 backlog 상한까지 계속 받고, 넘으면 429로 클라이언트에 넘긴다.

기동 중이라면 `LOGGYTRACY_STARTUP_RETRY_BUDGET`(기본 5분)까지 재시도하고 그 뒤 종료한다.
crash loop처럼 보이면 그 예산을 늘리는 것이 아니라 스토어를 고치는 것이 맞다.

### 기동이 "conditional writes" 오류로 거부된다

프리플라이트가 **조건부 쓰기가 강제되지 않는 스토어**를 감지한 것이다. 그대로 운영하면
manifest lost update가 나고 그건 데이터 손실이다.

S3 호환 스토어라면 `OBJECT_STORE_CONDITIONAL_PUT=etag`를 설정한다. 개발용 단일 프로세스라면
`file://` URL을 쓴다 — 그쪽은 CAS를 의도적으로 포기한다.

### 디스크가 찬다

```
du -sh $LOGGYTRACY_DATA_DIR/*        # wal / parts / traces 중 어디인가
```

- **parts/traces가 크다** → `CACHE_MAX_BYTES`를 줄인다. 캐시일 뿐이므로 지워도 S3에서 복원된다.
  단, `RETENTION_PERIOD`가 미설정이면 S3 쪽은 **아무것도 지워지지 않는다** — 그것부터 정한다.
- **WAL이 크다** → flush가 진행되지 않고 있다. 위 항목으로.
- stream index는 캐시 eviction 대상이 아니다. 라벨 카디널리티가 폭발하면 **evict 불가능한
  디스크 사용량**이 되므로, ingest 쪽 라벨을 고치는 것 외에 방법이 없다.

### 테넌트 삭제가 끝나지 않는다

`retention_rewrite_skipped_total`이 증가하면, 어떤 part가 `MERGE_MAX_MEMORY_BYTES` 안에서
재작성되지 않고 있다는 뜻이다. 쿼리에는 이미 안 보이지만 **바이트는 남아 있다.**

`MERGE_MAX_MEMORY_BYTES`를 올리거나 `ROW_GROUP_SIZE`를 줄인다(윈도 단위가 작아진다).

### 두 인스턴스가 같은 prefix에 떴다

구 인스턴스 로그에 `fenced by a newer writer`가 찍히고 종료 코드 1로 죽는다. 이것은 방어가
작동한 것이지 사고가 아니다. 다만:

- **구 인스턴스의 디스크를 버리면 안 된다.** 미flush 데이터가 그 WAL에 있다.
- 신 인스턴스는 정상 동작한다.
- 구 데이터를 살리려면 신 인스턴스를 내리고, 구 인스턴스를 그 디스크에서 재기동해 flush를
  완료시킨 뒤, 정상 절차로 교체한다.

---

## 계획된 장비 교체

순서를 지키면 무손실이다. **순서를 어기면 fencing이 구 인스턴스를 죽이므로**, 어긴 사실을
모르고 지나가지는 않는다.

1. 구 인스턴스에 `SIGTERM`. drain이 시작되고 ingest가 503을 반환한다.
2. `curl /metrics | grep pending_flush_bytes`가 **0이 되고** `force_flush_complete`가 1이 될
   때까지 기다린다. 오래 걸리면 로그에 운영자 경고가 찍힌다. **기다리는 것이 맞다.**
3. 프로세스가 스스로 종료한다. 종료 코드 0이면 모든 ack된 데이터가 durable하다.
   **0이 아니면 디스크를 버리지 않는다.**
4. 그 다음에 신 인스턴스를 띄운다.

3번의 종료 코드가 유일한 판단 근거다. SIGKILL에는 종료 코드가 없으므로, 2번을 건너뛰고
강제 종료했다면 그 디스크에서 재기동해 복구해야 한다.

## 강제 종료 후 복구

WAL은 그대로다. **같은 디스크**에서 재기동하면 replay가 미flush 데이터를 되살린다.

at-least-once이므로 flush 경계에서 중단된 경우 **일부 로그가 중복**될 수 있다. 이것은 의도된
트레이드오프이며(`ARCHITECTURE.md`), 현재 중복을 관측할 수단이 없다는 것이 알려진 공백이다.

## 백업

S3가 source of truth다. 로컬 디스크는 캐시 + 미flush WAL이다.

- 오브젝트 스토어 쪽 버저닝/복제 정책은 **스토어에서** 설정한다. 엔진은 관여하지 않는다.
- manifest 객체 하나가 전체 part 목록이다. 이것을 잃으면 part 객체가 남아 있어도 카탈로그가
  사라진다. **버저닝을 켜 두는 것을 강력히 권한다.**
- 로컬 디스크 백업은 의미가 없다 — 캐시이거나, 아직 durable하지 않은 데이터다.
