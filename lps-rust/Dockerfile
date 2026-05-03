FROM rust:1.95.0 AS builder

WORKDIR /app

COPY src ./src
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release

FROM debian:trixie-slim AS runner

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ /usr/local/bin/

CMD ["device"]

# docker build -t lps-rs .