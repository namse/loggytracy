# TODO

M3의 현재 범위 밖으로 미뤄 둔 작업과 후속 마일스톤 작업을 정리한다.

프로덕션 레디 게이트 전체 목록은 [`docs/PRODUCTION_READINESS_REVIEW.md`](docs/PRODUCTION_READINESS_REVIEW.md)에 있다.

## P0 — 프로덕션 게이트

- [ ] **WAL compaction wedge 수정** (아래 P5의 BLOCKER와 같은 항목). 재시작으로도 복구되지 않으므로
      기존 wedge 복구 절차(`journal.wal.compact.state` 수동 삭제)도 함께 문서화한다.
- [ ] **ingest backpressure**: memtable/WAL backlog 상한 초과 시 journal append 이전에 `429`.
      선행 작업으로 memtable 크기 O(1) 추적이 필요하다 (현재 `approximate_size`가 O(rows)이고
      500ms마다 ingest와 같은 락을 잡는다).
- [ ] **테넌시**: `X-Scope-OrgID` 추출 → 저장 경로 분할 축 → 테넌트별 스로틀·quota →
      테넌트 라벨 metrics. 설계는 `docs/ARCHITECTURE.md`의 "테넌시" 절.
- [x] TLS 미지원을 아키텍처 결정으로 명문화
- [x] ingest 입력 제한 (body/압축 해제 길이/라인/라벨 개수·길이/타임스탬프 수용 윈도우)

## P1 — LogQL 기능 보강

- [ ] `line_format`, `label_format` 지원
- [ ] `unwrap` 및 `quantile_over_time` 지원
- [ ] binary/vector 연산자 지원
- [ ] `without`, offset, subquery 지원
- [ ] JSON의 top-level array와 `null` 값에 대한 Loki 호환 semantics 지원
- [ ] 빈 문자열 equality, stream-label field, `_extracted` 충돌 이름에 대한 exact-field pruning 개선

## P2 — 정확성·스토리지 성능

- [ ] crash replay로 발생할 수 있는 중복 로그 deduplication
- [ ] Parquet range read 도입
- [ ] 메트릭 평가를 bounded in-memory 계산에서 streaming/pre-aggregation 방식으로 개선
- [ ] 실제 S3 또는 S3-compatible endpoint를 이용한 배포 환경 검증

## P3 — M5 운영 검증

- [ ] compaction 튜닝
- [ ] `merge_max_input_bytes`(압축 크기)와 `merge_max_memory_bytes`(압축 해제 크기)의 단위 불일치 수정.
      기본값으로 1 KiB 라인이면 대형 그룹 merge가 영구 실패하고 더 작은 그룹으로 물러서는 fallback이 없다.
- [x] retention 정책과 만료 데이터 삭제 구현 (retention 전용 타임아웃 knob 분리 포함)
- [ ] 쿼리 메모리·범위·동시성 등 resource limit을 운영 목표에 맞게 조정
- [ ] 명시적인 처리량·지연시간·메모리 목표를 정하고 부하 테스트 수행
- [ ] 부하 테스트 결과와 병목 구간을 문서화

## P4 — M6 장비 교체

상세 계획: [`docs/M6_IMPLEMENTATION_PLAN.md`](docs/M6_IMPLEMENTATION_PLAN.md)

- [x] graceful shutdown 핸들러 구현 (SIGTERM/SIGINT 수신 시 drain 시퀀스 시작, 시퀀스가 프로세스 종료 담당)
- [x] ingest 차단: draining 중 Loki push 503, OTLP UNAVAILABLE (journal append 이전)
- [x] in-flight drain: axum `with_graceful_shutdown` + tonic `serve_with_shutdown`
- [x] background 워커(flush/merge/retention/eviction) 정상 종료 후 최종 force-flush
- [x] force-flush 함수 구현: 임계값 무시하고 MemTable·pending checkpoint 소진, S3 업로드/manifest 갱신 완료 대기
- [x] object-store 지속 실패 시 무한 재시도 + stdout 경고 + 운영자 stdin 입력으로만 종료 (하드 타임아웃 없음)
- [x] 강제 종료 후 재시작 시 저널 replay로 무손실 자동 복구
- [x] drain-status readiness: draining 중 `/ready` 503 + `/metrics`에 pending bytes/flush 완료 노출
- [x] 장비 교체 리허설 (새 인스턴스가 무손실로 트래픽 재개)
- [ ] fresh-context 리뷰 (남은 게이트)

## P5 — M7 로컬 S3 부하 검증

상세 계획: [`docs/M7_IMPLEMENTATION_PLAN.md`](docs/M7_IMPLEMENTATION_PLAN.md)

- [x] 관측성 gauge 보강 (merge debt gauge 추가; active part 수·WAL backlog·memtable bytes는 기존 `/metrics`에 존재)
- [x] Tier B: `LatencyFaultStore` + `from_url` opt-in 래핑 (인프로세스 지연·장애 주입, 시드 재현)
- [x] Tier C: `docker-compose.yml` MinIO + `scripts/run_load_s3.sh` (실제 S3 프로토콜)
- [x] MinIO manifest CAS(`PutMode::Create`/`Update`) 동작 확인 (`OBJECT_STORE_CONDITIONAL_PUT=etag` 필요, 문서화 완료)
- [x] 부하 하네스 개선: target-rate pacing, warmup/steady-state 분리, 강제 eviction→restore, 목표 대비 pass/fail (`src/bin/load.rs`)
- [x] `docs/M7_LOAD_RESULTS.md` 결과·머신 프로파일·병목 문서화
- [ ] **BLOCKER — WAL compaction 무한 wedge 버그 수정 (후속 작업으로 분리):** 첫 compaction 이후
  phase-2 compaction-state 파일이 제거되지 않아, 좌표계가 리셋된 다음 compaction offset이
  stale offset과 비교되어 `"WAL compaction checkpoint moved backwards"`로 flush 루프가 영구
  wedge됨. 장애 주입 없이 Tier B(`file://`)·Tier C(MinIO) 양쪽에서 재현. object-store 백엔드
  런에서만 발생(로컬 전용은 `set_checkpoint` 경로라 무관). 이 수정 전에는 M7 수용 기준의
  "무손실 회복 + bounded backlog" 런을 통과할 수 없음.

