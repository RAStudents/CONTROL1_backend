FROM rust:1.93-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release && cp target/release/control-backend /usr/local/bin/control-backend

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 control && mkdir /exports && chown control:control /exports
COPY --from=build /usr/local/bin/control-backend /usr/local/bin/control-backend
USER control
ENTRYPOINT ["control-backend"]
