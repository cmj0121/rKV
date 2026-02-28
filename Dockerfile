# --- Builder stage ---
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY rkv/Cargo.toml rkv/Cargo.toml
COPY rkv-ffi/Cargo.toml rkv-ffi/Cargo.toml

# Stub lib files so cargo can resolve the workspace and cache dependencies
RUN mkdir -p rkv/src rkv-ffi/src \
    && echo "fn main() {}" > rkv/src/main.rs \
    && echo "" > rkv/src/lib.rs \
    && echo "" > rkv-ffi/src/lib.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl -p rkv --features server \
    || true

# Copy real source and build
COPY rkv/src rkv/src
COPY rkv/tests rkv/tests
COPY rkv-ffi/src rkv-ffi/src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl -p rkv --features server \
    && musl-strip /src/target/x86_64-unknown-linux-musl/release/rkv \
    && cp /src/target/x86_64-unknown-linux-musl/release/rkv /rkv

# --- Runtime stage ---
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /rkv /rkv

EXPOSE 8321
VOLUME /data

ENTRYPOINT ["/rkv", "serve", "--bind", "0.0.0.0", "--db", "/data", "--allow-all"]
