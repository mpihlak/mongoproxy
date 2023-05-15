# Needed for rust cross compilation helper scripts.
# https://github.com/tonistiigi/xx#rust
FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM rust:1.61-buster as builder

COPY --from=xx / /

WORKDIR /build/mongoproxy

COPY Cargo.* ./
COPY proxy/ ./proxy
COPY mongo-protocol/ ./mongo-protocol
COPY async-bson/ ./async-bson

RUN apt-get update && apt-get install -y clang lld

RUN cargo test --release
ARG TARGETPLATFORM

RUN xx-apt-get install -y xx-c-essentials
RUN xx-cargo build --release --target-dir ./

FROM debian:buster-slim

COPY --from=xx / /
RUN apt-get update && apt-get install -y iptables
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy

RUN adduser --uid 9999 --disabled-password --gecos '' mongoproxy
USER mongoproxy

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/*/release/mongoproxy ./
COPY iptables-init.sh .

ENV MALLOC_ARENA_MAX 2
