FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends pgbouncer postgresql-client \
    && rm -rf /var/lib/apt/lists/*
COPY entrypoint.sh /usr/local/bin/contract-pgbouncer
USER nobody
ENTRYPOINT ["sh", "/usr/local/bin/contract-pgbouncer"]
