# --- Builder stage ---
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY rkv/Cargo.toml rkv/Cargo.toml
COPY rkv-ffi/Cargo.toml rkv-ffi/Cargo.toml
COPY rill/Cargo.toml rill/Cargo.toml
COPY knot/Cargo.toml knot/Cargo.toml

# Stub lib files so cargo can resolve the workspace and cache dependencies
RUN mkdir -p rkv/src rkv-ffi/src rill/src knot/src \
    && echo "fn main() {}" > rkv/src/main.rs \
    && echo "" > rkv/src/lib.rs \
    && echo "" > rkv-ffi/src/lib.rs \
    && echo "fn main() {}" > rill/src/main.rs \
    && echo "" > rill/src/lib.rs \
    && echo "fn main() {}" > knot/src/main.rs \
    && echo "" > knot/src/lib.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl --bin knot \
    || true

# Copy real source and build
COPY rkv/src rkv/src
COPY rkv/tests rkv/tests
COPY rkv-ffi/src rkv-ffi/src
COPY knot/src knot/src
COPY knot/tests knot/tests

# Touch source to invalidate cargo fingerprints from the dependency-cache step
RUN find knot/src -name '*.rs' -exec touch {} + \
    && find rkv/src -name '*.rs' -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl --bin knot \
    && strip /src/target/x86_64-unknown-linux-musl/release/knot \
    && cp /src/target/x86_64-unknown-linux-musl/release/knot /knot

# --- Runtime stage ---
FROM busybox:musl

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /knot /usr/local/bin/knot

EXPOSE 8400
VOLUME /data

ENTRYPOINT ["knot"]
CMD ["serve", "/data", "--port", "8400"]
