FROM rust:1-bookworm AS build
WORKDIR /src

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY benches ./benches
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && : > src/lib.rs \
    && cargo build --release --bin collecty \
    && rm -rf src

COPY src ./src
RUN touch src/lib.rs src/main.rs && cargo build --release --bin collecty

FROM debian:bookworm-slim
RUN groupadd --system --gid 10002 collecty \
    && useradd --system --create-home --uid 10002 --gid 10002 collecty
USER collecty

WORKDIR /var/lib/collecty
VOLUME ["/var/lib/collecty", "/run/collecty"]
ENV COLLECTY_DATA_DIR=/var/lib/collecty
ENV COLLECTY_SOCKET_PATH=/run/collecty/otlp.sock
ENV COLLECTY_LOG_FORMAT=json

COPY --from=build /src/target/release/collecty /usr/local/bin/collecty
ENTRYPOINT ["/usr/local/bin/collecty"]
