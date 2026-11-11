#!/bin/sh
# These fixed resources are disposable: integration fixtures truncate/purge them.
set -eu
docker compose up -d --wait postgres rabbitmq
docker compose exec -T postgres psql -U payments -d postgres -v ON_ERROR_STOP=1 <<'SQL'
SELECT 'CREATE DATABASE payments_test' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'payments_test')\gexec
SQL
if ! docker compose exec -T rabbitmq rabbitmqctl list_vhosts -q | grep -Fxq payments_test; then
    docker compose exec -T rabbitmq rabbitmqctl add_vhost payments_test
fi
docker compose exec -T rabbitmq rabbitmqctl set_permissions -p payments_test payments '.*' '.*' '.*'
PAYMENT_DATABASE_URL=postgresql+asyncpg://payments:payments@127.0.0.1:55432/payments_test uv run alembic upgrade head
