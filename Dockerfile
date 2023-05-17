FROM rust:1.61-buster as builder

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /build/mongoproxy

COPY Cargo.* ./
COPY proxy/ ./proxy
COPY mongo-protocol/ ./mongo-protocol
COPY async-bson/ ./async-bson

RUN cargo build --release
RUN cargo test --release

FROM debian:buster-slim

RUN apt-get update && apt-get install -y iptables
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy

RUN adduser --uid 9999 --disabled-password --gecos '' mongoproxy
USER mongoproxy

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/release/mongoproxy ./
COPY iptables-init.sh .

ENV MALLOC_ARENA_MAX 2
