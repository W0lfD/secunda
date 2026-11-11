# Платёжный сервис

FastAPI принимает платёж, PostgreSQL хранит его и outbox, RabbitMQ доставляет работу consumer. Gateway эмулируется; реального списания нет. Нужны Docker Compose, Python 3.12, uv 0.11.27 и make. Локальные порты слушают только 127.0.0.1. Значения в `.env.example` предназначены только для локальной демонстрации.

```mermaid
flowchart LR
  Client -->|POST / GET| API --> DB[(PostgreSQL: payments + outbox)]
  DB --> Relay --> Rabbit[(RabbitMQ)] --> Consumer --> DB
  Consumer --> Gateway[Эмулятор gateway]
  Consumer --> Callback[Webhook получатель]
  Rabbit --> DLQ[payments.dlq]
```

## Запуск и демонстрация

```sh
cp .env.example .env
docker compose --profile demo up -d --build --wait
docker compose ps
```

`migrate` выполняется перед API и consumer; профиль `demo` запускает получатель на порту 8080. `PAYMENT_WEBHOOK_ALLOWED_ORIGINS` — JSON-массив разрешённых origin; для demo разрешён `http://callback:8080`. Ключ из `.env` должен содержать минимум 32 символа после удаления пробелов по краям. Все HTTP endpoints, включая `/ready`, `/docs` и `/openapi.json`, требуют статический `X-API-Key`; Swagger UI не добавляет заголовок самостоятельно.

Учётные данные и строки подключения задаются в `.env`.

```bash
export KEY="$(uv run --env-file .env python -c 'import os; print(os.environ["PAYMENT_API_KEY"])')"
export BODY='{"amount":"12.34","currency":"USD","description":"Заказ","metadata":{"order":1},"webhook_url":"http://callback:8080/ok"}'
curl -sS -H "X-API-Key: $KEY" http://127.0.0.1:8000/ready
curl -sS -H "X-API-Key: $KEY" -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' -d "$BODY" http://127.0.0.1:8000/api/v1/payments
# Повтор вернёт тот же payment_id и текущий статус:
curl -sS -H "X-API-Key: $KEY" -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' -d "$BODY" http://127.0.0.1:8000/api/v1/payments
# Другая сумма с тем же ключом: HTTP 409
curl -sS -i -H "X-API-Key: $KEY" -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' -d "${BODY/12.34/13.34}" http://127.0.0.1:8000/api/v1/payments
# Подставьте payment_id из ответа:
curl -sS -H "X-API-Key: $KEY" http://127.0.0.1:8000/api/v1/payments/PAYMENT_UUID
curl -sS http://127.0.0.1:8080/        # полученные callback
```

Для повторов используйте новый `Idempotency-Key` и URL `http://callback:8080/flaky` (первый запрос вернёт 500) или `/fail` (все запросы вернут 500, затем событие попадёт в DLQ). Получатель хранит события в памяти до перезапуска. DLQ можно просмотреть через RabbitMQ UI `http://127.0.0.1:15673` с учётными данными из `.env` или без удаления сообщений:

```sh
uv run --env-file .env sh -c \
  'curl -sS -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -H "Content-Type: application/json" -d "$1" http://127.0.0.1:15673/api/queues/%2F/payments.dlq/get' sh \
  '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}'
```

```sh
docker compose --profile demo stop       # сохранить данные
docker compose --profile demo down       # удалить контейнеры, сохранить volumes
docker compose --profile demo down -v    # сбросить все локальные платежи и очереди
```

## Проверки

`make bootstrap` запускает PostgreSQL/RabbitMQ и идемпотентно создаёт `payments_test` DB и vhost, затем применяет миграции. Для bootstrap используйте порты по умолчанию 55432 и 5673. E2E создаёт отдельные Compose-проекты, порты и volumes и удаляет их после проверки.

```sh
make sync
make format lint typecheck
make unit
make bootstrap
make integration
make smoke
make audit
make logs                                # reports/compose.log
# или все тесты: make test
```

JUnit и coverage unit/integration сохраняются в `reports/`; при ошибке E2E выводит логи своего проекта до его удаления и сохраняет их в JUnit. Coverage контейнерных процессов не измеряется. CI выполняет те же цели и сохраняет отчёты и логи при ошибке.

## Поведение и ограничения

- POST требует `Idempotency-Key`, возвращает 202; повтор эквивалентного запроса возвращает тот же ID и текущий статус. Изменение тела — 409; неверный запрос — 422, ключ API — 401, неизвестный платёж — 404. Сумма положительная, до 18 цифр и двух знаков, RUB/USD/EUR; JSON-число и строка разбираются точно, в ответе сумма строка.
- Эмулятор работает 2–5 секунд и в 10% случаев возвращает конечный `failed`; callback отправляется и для `failed`. Обе ветви проверяются детерминированными тестами; Compose E2E принимает любой конечный статус.
- Три логические попытки отдельно для gateway и webhook, задержки 1 и 2 секунды. После трёх исключений обработки неизвестный результат остаётся `pending`, событие попадает в DLQ. Отказ webhook не меняет конечный статус. Ошибки БД и публикации повторяются отдельно; malformed/future/unknown события идут в native DLX.
- Доставка как минимум один раз: после сбоя HTTP может вызваться повторно с тем же `event_id`. Получатель должен дедуплицировать его. Блокировки строк удерживаются во время ограниченного I/O, что ограничивает пропускную способность. [Решение о транзакциях](docs/adr/001-transactions.md), [доставке и блокировках](docs/adr/002-delivery.md).
