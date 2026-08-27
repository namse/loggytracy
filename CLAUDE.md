# CLAUDE.md

## 커밋 메시지

짧은 영어로 쓴다. 형식은 `<area>: <동사원형> <목적어>`.

- subject는 50자 이내, 길어도 72자를 넘기지 않는다
- 소문자로 시작하고 마침표를 붙이지 않는다
- 명령형 현재시제를 쓴다 (`add`, `fix`, `remove`, `move`)
- subject에 서술문을 여러 개 이어붙이지 않는다. 이유나 배경은 body에 쓴다
- body는 필요할 때만 쓰고, subject와 빈 줄로 띄운다

area는 코드가 사는 곳을 쓴다: `metrics`, `logs`, `traces`, `wal`, `manifest`,
`compaction`, `query`, `api`, `bench`, `ci`, `docs`.

예시:

```
metrics: add read API routes
bench: fail closed on unknown digest class
wal: retire segments only after checkpoint
```

body가 필요한 경우:

```
metrics: fix series state accounting on abort

An abort that revived an evicted series dropped its state bytes,
and release past zero wrapped the gate into refusing everything.
Revive restores the bytes; release clamps at zero.
```
