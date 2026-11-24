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
    tar \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install curl-impersonate Chrome binary to bypass Cloudflare TLS fingerprints
RUN curl -sSL https://github.com/lwthiker/curl-impersonate/releases/download/v0.6.1/curl-impersonate-v0.6.1.x86_64-linux-gnu.tar.gz | tar -xz -C /usr/local/bin && chmod +x /usr/local/bin/curl*

WORKDIR /app

COPY --from=builder /usr/src/kick_miner/target/release/kick_miner /usr/local/bin/kick_miner
COPY config*.json ./

ENV PORT=10000
EXPOSE 10000

CMD ["/usr/local/bin/kick_miner"]
