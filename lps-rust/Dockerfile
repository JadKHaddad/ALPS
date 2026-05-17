FROM rust:1.95.0 AS builder

WORKDIR /app

COPY src ./src
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release

FROM debian:trixie-slim AS runner

WORKDIR /app

ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.29.7

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" | \
    tar zxvf - --strip-components=1 -C /usr/local/bin/ docker/docker

RUN mkdir -p /usr/local/lib/docker/cli-plugins/ \
    && curl -fL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

COPY --from=builder /app/target/release/ /usr/local/bin/

CMD ["device"]

# docker build -t jadkhaddad/lps-rs:0.1.0 .
# docker push jadkhaddad/lps-rs:0.0.1