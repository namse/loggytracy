# TODO

M3의 현재 범위 밖으로 미뤄 둔 작업과 후속 마일스톤 작업을 정리한다.

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
- [ ] retention 정책과 만료 데이터 삭제 구현
- [ ] 쿼리 메모리·범위·동시성 등 resource limit을 운영 목표에 맞게 조정
- [ ] 명시적인 처리량·지연시간·메모리 목표를 정하고 부하 테스트 수행
- [ ] 부하 테스트 결과와 병목 구간을 문서화

## P4 — M6 고가용성

- [ ] read replica와 manifest following 구현
- [ ] fenced promotion을 포함한 master 승격 절차 구현
- [ ] 장비 교체 및 장애 복구 리허설 수행

