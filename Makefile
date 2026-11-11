UV ?= uv
UV_RUN = $(UV) run --env-file .env

.PHONY: sync format lint typecheck unit bootstrap integration smoke test audit logs
sync:
	$(UV) sync --locked
format:
	$(UV) run ruff format --check .
lint:
	$(UV) run ruff check .
typecheck:
	$(UV) run mypy src
unit:
	mkdir -p reports
	$(UV_RUN) pytest tests/unit --junitxml=reports/unit.xml --cov=payment_service --cov-report=xml:reports/unit-coverage.xml
bootstrap:
	$(UV_RUN) sh tools/bootstrap-test.sh
integration: bootstrap
	mkdir -p reports
	$(UV_RUN) pytest tests/integration --junitxml=reports/integration.xml --cov=payment_service --cov-report=xml:reports/integration-coverage.xml
smoke:
	mkdir -p reports
	RUN_COMPOSE_E2E=1 $(UV_RUN) pytest tests/e2e -o junit_logging=all --junitxml=reports/e2e.xml
test: unit integration smoke
audit:
	mkdir -p reports
	$(UV) export --locked --no-hashes --no-emit-project --group dev --format requirements-txt > reports/requirements.txt
	$(UV) run pip-audit --disable-pip --no-deps -r reports/requirements.txt
logs:
	mkdir -p reports
	docker compose logs --no-color > reports/compose.log
