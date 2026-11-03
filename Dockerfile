FROM rust:1.99-trixie as builder

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /build/mongoproxy

COPY Cargo.* ./
COPY proxy/ ./proxy
COPY mongo-protocol/ ./mongo-protocol
COPY async-bson/ ./async-bson

RUN cargo build --release
RUN cargo test --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y iptables
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy

RUN useradd --uid 9999 --create-home mongoproxy
USER mongoproxy

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/release/mongoproxy ./
COPY iptables-init.sh .

ENV MALLOC_ARENA_MAX 2
# A fixed mmap threshold stops glibc from raising it after large buffers are freed, so
# the memory of large messages goes back to the OS instead of staying in the heap.
ENV MALLOC_MMAP_THRESHOLD_ 131072
