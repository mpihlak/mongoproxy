FROM rust:1.37-buster as builder

WORKDIR /build
RUN USER=root cargo new --bin mongoproxy

WORKDIR /build/mongoproxy

COPY Cargo.* ./
RUN cargo build

# Clean up dummy project remains
RUN rm src/*.rs
RUN rm target/debug/deps/mongoproxy*

# Now, build mongoproxy
COPY src ./src
RUN cargo build

FROM debian:buster-slim

RUN apt-get update && apt-get install -y procps sysstat

WORKDIR /mongoproxy
COPY --from=builder /build/mongoproxy/target/debug/mongoproxy ./
