FROM rust:1.47.0-buster as builder

WORKDIR /build/mongoproxy

COPY Cargo.* ./
COPY proxy/ ./proxy
COPY mongo-protocol/ ./mongo-protocol

RUN cargo build --release

FROM debian:buster-slim

RUN apt-get update && apt-get install -y iptables
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy

RUN adduser --uid 9999 --disabled-password --gecos '' mongoproxy
USER mongoproxy

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/release/mongoproxy ./
COPY iptables-init.sh .
