.PHONY: help sync clean lint format format-check check test test-quiet test-slow all

help:
	@echo "Available commands:"
	@echo "  make sync       - Sync dependencies using uv"
	@echo "  make lint       - Run ruff linter"
	@echo "  make format     - Format code with ruff"
	@echo "  make check      - Run all checks (lint + format check)"
	@echo "  make test       - Run tests with pytest (shows output with -s)"
	@echo "  make test-quiet - Run tests without output"
	@echo "  make test-slow  - Run all tests including slow ones (with output)"
	@echo "  make all        - Run all checks and tests"
	@echo "  make clean      - Clean build artifacts"

sync:
	uv sync --all-extras

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

check: lint format-check

test:
	uv run pytest -s

test-quiet:
	uv run pytest

test-slow:
	uv run pytest -s -m slow

all: check test

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .venv/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
