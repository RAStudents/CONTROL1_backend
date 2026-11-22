#!/bin/sh
set -eu
umask 077
mkdir -p /tmp/contract-pgbouncer
escaped=$(printf '%s' "$PGPASSWORD" | sed 's/\\/\\\\/g; s/"/\\"/g')
printf '"control" "%s"\n' "$escaped" > /tmp/contract-pgbouncer/users.txt
cat > /tmp/contract-pgbouncer/pgbouncer.ini <<'INI'
[databases]
control = host=postgres port=5432 dbname=control
[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
unix_socket_dir = /tmp/contract-pgbouncer
pidfile = /tmp/contract-pgbouncer/pgbouncer.pid
auth_type = plain
auth_file = /tmp/contract-pgbouncer/users.txt
pool_mode = transaction
default_pool_size = 2
max_client_conn = 32
ignore_startup_parameters = extra_float_digits,options
server_reset_query = DISCARD ALL
server_reset_query_always = 1
INI
exec pgbouncer /tmp/contract-pgbouncer/pgbouncer.ini
