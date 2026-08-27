# obsy

An observability stack for a single machine, built as separate programs that
ship on their own.

| Component | Directory | What it is |
|---|---|---|
| **signy** | [`signy/`](signy/) | The storage and query engine. Ingests logs, traces and metrics over OTLP and answers a first-party HTTP API. Single machine, single writer, S3-compatible object storage as the source of truth, local disk as an evictable cache, and one declared memory budget divided into arenas. |
| **collecty** | [`collecty/`](collecty/) | The collector. Takes OTLP over a Unix domain socket, writes it zstd-compressed to an append-only disk queue, and ships it to signy in batches. It never decodes a payload: OTLP exports and zstd frames both survive concatenation, so a batch is a `memcpy`. |

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
├── collecty/            the collector: Cargo.toml, src, docs, benches
└── signy/               the engine: Cargo.toml, src, docs, benches, compare, deploy
```

There is no Cargo workspace at the root. Each component is self-contained under
its own directory, carries its own `Cargo.lock` and `rust-toolchain.toml`, and
builds on its own.

That was decided rather than inherited. A root workspace would share one
`Cargo.lock`, which means a dependency bumped for collecty changes what signy
builds — the opposite of components that ship on their own. It would also move
the lock out of `signy/`, breaking an image build that copies it from there.
Neither cost buys anything: the two crates share no code.

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

## Working on collecty

```sh
cd collecty
cargo test
```

The collector documents itself under [`collecty/docs/`](collecty/docs/):
[`ARCHITECTURE.md`](collecty/docs/ARCHITECTURE.md) for what it is and why each
decision was made, and
[`CONFIGURATION.md`](collecty/docs/CONFIGURATION.md) for every knob and what
raising it costs.

collecty carries no comments in its source. Every "why" that would have been one
is in `docs/ARCHITECTURE.md` instead.
