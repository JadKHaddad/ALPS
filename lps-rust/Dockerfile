FROM debian:trixie-slim

WORKDIR /app

ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.29.7
ARG RUST_VERSION=1.95.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    pkg-config \
    python3.13 \
    python3.13-venv \
    python3.13-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" | \
    tar zxvf - --strip-components=1 -C /usr/local/bin/ docker/docker

RUN mkdir -p /usr/local/lib/docker/cli-plugins/ \
    && curl -fL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain ${RUST_VERSION}

ENV PATH="/root/.cargo/bin:${PATH}"

ENV VIRTUAL_ENV="/opt/pyo3venv"

COPY models ./models
COPY pyo3-requirements.txt .

RUN python3.13 -m venv "$VIRTUAL_ENV" && \
    . "$VIRTUAL_ENV/bin/activate" && \
    pip install -r pyo3-requirements.txt

ENV PYO3_PYTHON="$VIRTUAL_ENV/bin/python3.13"

COPY .cargo ./.cargo
COPY src ./src
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release && \
    cp target/release/* /usr/local/bin/ 2>/dev/null; \
    find target/release -maxdepth 1 -type f -executable -exec cp {} /usr/local/bin/ \;

ENV PYTHONPATH="$VIRTUAL_ENV/lib/python3.13/site-packages"

CMD ["device"]

# docker build -t jadkhaddad/lps-rs:0.1.0 .
# docker push jadkhaddad/lps-rs:0.1.0