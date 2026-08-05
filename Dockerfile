FROM rust:latest AS builder

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock /src/
COPY gossip-monitor /src/gossip-monitor

RUN cargo build --release --locked -p gossip-monitor

FROM debian:trixie-slim AS runner

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data/gossip /opt/gossip-monitor \
    && chown -R 1000:1000 /data /opt/gossip-monitor

WORKDIR /opt/gossip-monitor
COPY --from=builder /src/target/release/gossip-monitor /opt/gossip-monitor/gossip-monitor

USER 1000

EXPOSE 8090

ENTRYPOINT ["/opt/gossip-monitor/gossip-monitor"]
