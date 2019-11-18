FROM rust:1.39-buster as builder

WORKDIR /build
RUN USER=root cargo new --bin mongoproxy

WORKDIR /build/mongoproxy

COPY Cargo.* ./
RUN mkdir benches && touch benches/tracker_benchmark.rs
RUN cargo build --release

# Clean up dummy project remains
RUN rm src/*.rs
RUN rm target/release/deps/mongoproxy*

# Now, build mongoproxy
COPY src/ ./src/
COPY benches/ ./benches/
RUN cargo build

FROM debian:buster-slim

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/release/mongoproxy ./
