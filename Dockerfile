# Build the release binary against a pinned toolchain, then ship it on a base
# with nothing but the CA bundle it needs to reach an S3 endpoint over TLS.
FROM rust:1-bookworm AS build
WORKDIR /src

# Dependencies first, so editing our own sources does not re-resolve or
# re-download the tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --bin loggytracy \
    && rm -rf src

COPY src ./src
# `build_info` and the buildinfo endpoint read this. Without it a scraped
# series cannot be attributed to code, which is the first question asked when
# two deployments behave differently.
ARG LOGGYTRACY_BUILD_REVISION=unknown
ARG LOGGYTRACY_BUILD_BRANCH=unknown
ENV LOGGYTRACY_BUILD_REVISION=${LOGGYTRACY_BUILD_REVISION}
ENV LOGGYTRACY_BUILD_BRANCH=${LOGGYTRACY_BUILD_BRANCH}
# Touch so the dependency-cache layer above does not make cargo skip our code.
RUN touch src/main.rs && cargo build --release --bin loggytracy

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# The data directory holds the WAL, and until a flush lands the WAL is the only
# copy of acknowledged data. It must be a persistent volume, and the volume must
# follow the pod. See docs/RUNBOOK.md.
RUN useradd --system --create-home --uid 10001 loggytracy
USER loggytracy
WORKDIR /var/lib/loggytracy
VOLUME ["/var/lib/loggytracy"]
ENV LOGGYTRACY_DATA_DIR=/var/lib/loggytracy

COPY --from=build /src/target/release/loggytracy /usr/local/bin/loggytracy

# The binary binds loopback by default, so that not configuring an address
# cannot silently expose a listener with no TLS and no authentication. An image
# exists to be reached, so it makes that decision explicitly here — which also
# keeps the decision visible to anyone reading the Dockerfile.
ENV LOGGYTRACY_LISTEN_ADDR=0.0.0.0:3100
ENV LOGGYTRACY_OTLP_GRPC_ADDR=0.0.0.0:4317
EXPOSE 3100 4317
# No TLS in this process by design; put it behind a proxy inside a trust
# boundary. See docs/ARCHITECTURE.md, "Transport security".
ENTRYPOINT ["/usr/local/bin/loggytracy"]
