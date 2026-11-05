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

synth:
	$(PYTHON) -m pip install -e ".[infra]"
	npx --yes aws-cdk@2.1147.0 synth
