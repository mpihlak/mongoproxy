FROM rust:1.39-buster as builder

WORKDIR /build
RUN USER=root cargo new --bin mongoproxy

WORKDIR /build/mongoproxy

COPY Cargo.* ./
RUN mkdir benches && touch benches/tracker_benchmark.rs
RUN cargo build --release

# Clean up dummy project remains
RUN rm src/*.rs
RUN rm target/*/deps/mongoproxy*
RUN rm target/*/mongoproxy

# Now, build mongoproxy
COPY src/ ./src/
COPY benches/ ./benches/
RUN cargo build --release --features=log_mongodb_messages

FROM debian:buster-slim

RUN apt-get update && apt-get install -y iptables
RUN update-alternatives --set iptables /usr/sbin/iptables-legacy

RUN adduser --uid 9999 --disabled-password --gecos '' mongoproxy
USER mongoproxy

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/release/mongoproxy ./
COPY iptables-init.sh .
