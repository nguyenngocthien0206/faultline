.DEFAULT_GOAL := help
.PHONY: help install lint fmt test test-integration check clean ref-up ref-down ref-ps ref-logs

COMPOSE := docker compose -f reference/docker-compose.yml

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-17s %s\n", $$1, $$2}'

install: ## Create .venv and install all dependencies from uv.lock
	uv sync --locked

lint: ## Run ruff lint and format checks
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint issues and format the code
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run the tests that need no Docker
	uv run pytest -m "not integration"

test-integration: ## Run the tests against the reference stack (start it with ref-up)
	FAULTLINE_REQUIRE_STACK=1 uv run pytest -m integration

check: lint test ## Lint and Docker-free tests, what CI runs on every OS

# Credentials live in reference/.env (gitignored). Created from the example on
# first use and never overwritten, so local edits survive.
reference/.env:
	cp reference/.env.example $@

ref-up: reference/.env ## Build and start the reference stack, wait until it is healthy
	$(COMPOSE) up -d --build --wait

ref-down: reference/.env ## Stop the reference stack and delete its volumes
	$(COMPOSE) down -v

ref-ps: reference/.env ## Show the reference stack services
	$(COMPOSE) ps -a

ref-logs: reference/.env ## Follow the reference stack logs
	$(COMPOSE) logs -f --tail 100

clean: ## Remove caches, build output and faultline workspaces
	rm -rf .pytest_cache .ruff_cache dist build .faultline
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} +
