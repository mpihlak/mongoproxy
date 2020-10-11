FROM rust:1.47.0-buster as builder

WORKDIR /build
RUN USER=root cargo new --bin mongoproxy

WORKDIR /build/mongoproxy

COPY Cargo.* ./
RUN cargo build --release

# Clean up dummy project remains
RUN rm src/*.rs
RUN rm target/*/deps/mongoproxy*
RUN rm target/*/mongoproxy

# Now, build and test mongoproxy
COPY src/ ./src/
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
