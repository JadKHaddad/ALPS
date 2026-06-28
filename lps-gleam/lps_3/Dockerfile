ARG ERLANG_VERSION=28.0.2.0
ARG GLEAM_VERSION=v1.14.0

FROM ghcr.io/gleam-lang/gleam:${GLEAM_VERSION}-scratch AS gleam

FROM erlang:${ERLANG_VERSION}-alpine AS build
COPY --from=gleam /bin/gleam /bin/gleam
RUN apk add --no-cache git
COPY . /gleam/
RUN cd /gleam && gleam export erlang-shipment

FROM erlang:${ERLANG_VERSION}-alpine

RUN apk add --no-cache iptables

COPY --from=build /gleam/build/erlang-shipment /gleam

WORKDIR /app

ENTRYPOINT ["/gleam/entrypoint.sh"]

CMD ["run"]

# docker build -t jadkhaddad/lps-gleam:0.1.0 .
# docker push jadkhaddad/lps-gleam:0.1.0

# docker run -it --rm -v $(pwd):/app -e ERL_FLAGS="-sname node_1@localhost -setcookie secret" jadkhaddad/lps-gleam:0.1.0 run --config-file lps3-config-1.yaml --bootstrap