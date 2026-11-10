.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-unit check clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

install: ## Create .venv and install all dependencies from uv.lock
	uv sync --locked

lint: ## Run ruff lint and format checks
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint issues and format the code
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run the full test suite
	uv run pytest

test-unit: ## Run tests without the slower integration ones
	uv run pytest -m "not integration"

check: lint test ## Everything CI runs

clean: ## Remove caches, build output and faultline workspaces
	rm -rf .pytest_cache .ruff_cache dist build .faultline
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} +
