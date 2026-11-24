# Build Stage
FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/kick_miner

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/kick_miner/target/release/kick_miner /usr/local/bin/kick_miner
COPY config*.json ./

ENV PORT=10000
EXPOSE 10000

CMD ["/usr/local/bin/kick_miner"]
