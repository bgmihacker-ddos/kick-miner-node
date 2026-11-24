# Build Stage
FROM rust:1.80-bookworm AS builder

WORKDIR /usr/src/kick_miner

COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || true
RUN rm -rf src target/release/deps/kick_miner*

COPY src ./src
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/kick_miner/target/release/kick_miner /usr/local/bin/kick_miner
COPY config*.json ./

ENV PORT=10000
EXPOSE 10000

CMD ["/usr/local/bin/kick_miner"]
