# obsy

An observability stack for a single machine, built as separate programs that
ship on their own.

| Component | Directory | What it is |
|---|---|---|
| **signy** | [`signy/`](signy/) | The storage and query engine. Ingests logs, traces and metrics over OTLP and answers a first-party HTTP API. Single machine, single writer, S3-compatible object storage as the source of truth, local disk as an evictable cache, and one declared memory budget divided into arenas. |
| **collecty** | — | The collector. Not written yet; it will sit in `collecty/` beside signy. |

`obsy` is the product and the name of this repository. It is deliberately not a
binary, a crate, an env prefix, a metric family or an image — nothing at build
or run time is called obsy. Each component owns its name everywhere it is
visible: signy's knobs are `SIGNY_*`, its metric families are `signy_*`, its
routes are `/signy/...`, its headers are `X-Signy-*` and its image is
`ghcr.io/namse/signy`. Image tags do not derive from the repository name, so
signy's image and collecty's stay distinct.

## Layout

```
obsy/
├── .github/workflows/   one workflow per component
├── CLAUDE.md            repository conventions
└── signy/               the engine: Cargo.toml, src, docs, benches, compare, deploy
```

There is no Cargo workspace at the root. Each component is self-contained under
its own directory and builds on its own. Whether signy and collecty share a
workspace is a decision for the commit that adds collecty.

## Working on signy

```sh
cd signy
cargo test
```

The engine documents itself under [`signy/docs/`](signy/docs/):
[`ARCHITECTURE.md`](signy/docs/ARCHITECTURE.md) for what it is,
[`VISION.md`](signy/docs/VISION.md) for what it is for and what would falsify
the claim, [`QUERY_API.md`](signy/docs/QUERY_API.md) for the API contract,
[`CONFIGURATION.md`](signy/docs/CONFIGURATION.md) for every knob, and
[`DEPLOYMENT.md`](signy/docs/DEPLOYMENT.md) with
[`RUNBOOK.md`](signy/docs/RUNBOOK.md) for running it.
