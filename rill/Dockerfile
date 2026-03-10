# --- Builder stage ---
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY rkv/Cargo.toml rkv/Cargo.toml
COPY rkv-ffi/Cargo.toml rkv-ffi/Cargo.toml
COPY rill/Cargo.toml rill/Cargo.toml

# Stub lib files so cargo can resolve the workspace and cache dependencies
RUN mkdir -p rkv/src rkv-ffi/src rill/src \
    && echo "fn main() {}" > rill/src/main.rs \
    && echo "" > rill/src/lib.rs \
    && echo "fn main() {}" > rkv/src/main.rs \
    && echo "" > rkv/src/lib.rs \
    && echo "" > rkv-ffi/src/lib.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl -p rill --bin rill \
    || true

# Copy real source and build
COPY rill/src rill/src
COPY rkv/src rkv/src
COPY rkv-ffi/src rkv-ffi/src

# Touch source to invalidate cargo fingerprints from the dependency-cache step
RUN find rill/src -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl -p rill --bin rill \
    && strip /src/target/x86_64-unknown-linux-musl/release/rill \
    && cp /src/target/x86_64-unknown-linux-musl/release/rill /rill

# --- Runtime stage ---
FROM busybox:musl

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /rill /usr/local/bin/rill

EXPOSE 3000

ENTRYPOINT ["rill"]
CMD ["serve"]
