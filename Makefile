.PHONY: install seed dev test lint fmt up down

up:
	docker compose up -d

down:
	docker compose down

install:
	poetry install

seed:
	poetry run python packages/pipeline/scripts/seed_source.py

dev:
	cd packages/pipeline && poetry run dagster dev -m pipeline.definitions --working-directory .

test:
	poetry run pytest packages/connectors/tests packages/pipeline/tests -v

lint:
	poetry run ruff check .

fmt:
	poetry run ruff format .
