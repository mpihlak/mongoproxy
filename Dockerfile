FROM rust:1.37-buster as builder

WORKDIR /build
COPY Cargo.* ./
COPY src ./src

RUN cargo update
RUN cargo build

FROM debian:buster-slim

WORKDIR /mongoproxy
COPY --from=builder /build/target/debug/mongoproxy .
