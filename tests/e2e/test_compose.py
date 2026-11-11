"""Isolated Compose acceptance. Run with RUN_COMPOSE_E2E=1."""

import json
import os
import subprocess
import time
from uuid import uuid4

import httpx
import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.getenv("RUN_COMPOSE_E2E") != "1", reason="set RUN_COMPOSE_E2E=1"),
]
KEY = os.getenv("PAYMENT_API_KEY", "")


def test_compose_recovers_pending_work_and_delivers_callbacks():
    project = "payments-e2e-" + uuid4().hex[:12]
    env = {
        **os.environ,
        "PAYMENT_API_KEY": KEY,
        "PAYMENT_WEBHOOK_ALLOWED_ORIGINS": '["http://callback:8080"]',
        "POSTGRES_PORT": "0",
        "RABBIT_PORT": "0",
        "RABBIT_MANAGEMENT_PORT": "0",
        "API_PORT": "0",
        "CALLBACK_PORT": "0",
    }
    base = ["docker", "compose", "-p", project, "--profile", "test"]

    def compose(*args):
        return subprocess.run(
            [*base, *args], env=env, check=True, text=True, capture_output=True, timeout=240
        ).stdout

    def port(service, number):
        return int(compose("port", service, str(number)).strip().rsplit(":", 1)[1])

    def until(fn, timeout=100):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = fn()
                if last:
                    return last
            except (httpx.HTTPError, KeyError):
                pass
            time.sleep(0.5)
        raise AssertionError(f"timed out waiting; last result: {last!r}")

    try:
        compose("up", "-d", "--build", "--wait", "--scale", "consumer=2")
        api = f"http://127.0.0.1:{port('api', 8000)}"
        callback = f"http://127.0.0.1:{port('callback', 8080)}"
        headers = {"X-API-Key": KEY}

        def create(path):
            response = httpx.post(
                api + "/api/v1/payments",
                headers={**headers, "Idempotency-Key": uuid4().hex},
                json={
                    "amount": "12.34",
                    "currency": "USD",
                    "description": "acceptance",
                    "metadata": {},
                    "webhook_url": "http://callback:8080/" + path,
                },
                timeout=5,
            )
            assert response.status_code == 202, response.text
            return response.json()["payment_id"]

        def events(payment):
            return [
                event["body"]
                for event in httpx.get(callback, timeout=5).json()
                if event["body"]["payment_id"] == payment
            ]

        # Commit while the broker is unavailable, then restart both persistent services.
        compose("stop", "consumer", "rabbitmq")
        pending = create("ok")
        compose("stop", "postgres")
        compose("start", "postgres", "rabbitmq")
        compose("start", "consumer")
        received = until(lambda: events(pending))
        detail = httpx.get(api + "/api/v1/payments/" + pending, headers=headers, timeout=5).json()
        assert detail["status"] in ("succeeded", "failed")
        assert received[0]["status"] == detail["status"]
        assert received[0]["amount"] == "12.34"

        flaky = create("flaky")
        attempts = until(lambda: result if len(result := events(flaky)) >= 2 else None)
        assert len({event["event_id"] for event in attempts}) == 1
        assert len({event["status"] for event in attempts}) == 1
        failed = create("fail")
        attempts = until(lambda: result if len(result := events(failed)) >= 3 else None)
        assert len({event["event_id"] for event in attempts}) == 1
        management = f"http://127.0.0.1:{port('rabbitmq', 15672)}"

        def dead_messages():
            response = httpx.post(
                management + "/api/queues/%2F/payments.dlq/get",
                auth=(env["RABBITMQ_USER"], env["RABBITMQ_PASSWORD"]),
                json={"count": 10, "ackmode": "ack_requeue_true", "encoding": "auto"},
                timeout=5,
            )
            response.raise_for_status()
            return response.json()

        payloads = [json.loads(message["payload"]) for message in until(dead_messages)]
        assert len(payloads) == 1
        assert payloads[0]["payment_id"] == failed and payloads[0]["work_version"] == 3
    except Exception:
        print(
            subprocess.run(
                [*base, "logs", "--no-color", "--tail=100"],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            ).stdout
        )
        raise
    finally:
        subprocess.run([*base, "down", "-v", "--remove-orphans"], env=env, check=True, timeout=120)


def test_rabbitmq_message_survives_container_recreation():
    project = "payments-e2e-" + uuid4().hex[:12]
    env = {**os.environ, "RABBIT_PORT": "0", "RABBIT_MANAGEMENT_PORT": "0"}
    base = ["docker", "compose", "-p", project]

    def compose(*args):
        return subprocess.run(
            [*base, *args], env=env, check=True, text=True, capture_output=True, timeout=240
        ).stdout

    def management():
        port = compose("port", "rabbitmq", "15672").strip().rsplit(":", 1)[1]
        return f"http://127.0.0.1:{port}/api"

    auth = (env["RABBITMQ_USER"], env["RABBITMQ_PASSWORD"])
    try:
        compose("up", "-d", "--wait", "rabbitmq")
        api = management()
        queue = httpx.put(
            api + "/queues/%2F/persistence-probe", auth=auth, json={"durable": True}, timeout=5
        )
        assert queue.status_code in (201, 204), queue.text
        published = httpx.post(
            api + "/exchanges/%2F/amq.default/publish",
            auth=auth,
            json={
                "properties": {"delivery_mode": 2},
                "routing_key": "persistence-probe",
                "payload": "persisted message",
                "payload_encoding": "string",
            },
            timeout=5,
        )
        assert published.json() == {"routed": True}
        compose("up", "-d", "--force-recreate", "--wait", "rabbitmq")
        response = httpx.post(
            management() + "/queues/%2F/persistence-probe/get",
            auth=auth,
            json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"},
            timeout=5,
        )
        assert response.status_code == 200, response.text
        messages = response.json()
        assert len(messages) == 1 and messages[0]["payload"] == "persisted message"
        assert messages[0]["properties"]["delivery_mode"] == 2
    except Exception:
        print(
            subprocess.run(
                [*base, "logs", "--no-color", "--tail=100"],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            ).stdout
        )
        raise
    finally:
        subprocess.run([*base, "down", "-v", "--remove-orphans"], env=env, check=True, timeout=120)
