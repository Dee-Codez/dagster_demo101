.PHONY: install seed dev dev-acme test lint fmt up down

up:
	docker compose up -d

down:
	docker compose down

install:
	poetry install

seed:
	poetry run python packages/pipeline/scripts/seed_source.py

dev:
	poetry run dagster dev -m pipeline.definitions

dev-acme:
	PLATFORM_CLIENT_ID=acme poetry run dagster dev -m pipeline.definitions

test:
	poetry run pytest packages/connectors/tests packages/pipeline/tests -v

lint:
	poetry run ruff check .

fmt:
	poetry run ruff format .
