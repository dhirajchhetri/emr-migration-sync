PYTHON ?= python3

.PHONY: test lint format demo

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check src tests jobs infra
	$(PYTHON) -m ruff format --check src tests jobs infra

format:
	$(PYTHON) -m ruff format src tests jobs infra

demo:
	$(PYTHON) -m emr_sync.cli demo
