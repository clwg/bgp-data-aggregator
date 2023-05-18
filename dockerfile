FROM rust:latest as build

RUN rustup target add x86_64-unknown-linux-musl
RUN apt-get update && apt-get install -y musl-tools

COPY . /app
WORKDIR /app

RUN cargo build --target=x86_64-unknown-linux-musl --release