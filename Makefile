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

# EDITION=naive or EDITION=hardened runs one share of the suite, as the two CI
# jobs do: hardened takes the tests marked `hardened`, naive takes the rest
# (including the edition-free stack checks). Unset: the whole suite.
INTEGRATION_MARKS := integration
INTEGRATION_MARKS_naive := integration and not hardened
INTEGRATION_MARKS_hardened := integration and hardened

# WORKERS pytest-xdist processes share the suite; a test class (or a module
# outside classes) is the unit one worker takes (tests/reference/conftest.py).
# Keep WORKERS at least the number of units (7 today): xdist queues a second
# unit on a worker that has two tests or fewer left, so with fewer workers one
# seven-run experiment can wait behind another. With fewer, units start in
# file order (--no-loadscope-reorder), which puts the long ones first.
# On Docker Desktop with 16 CPUs, nine concurrent runs take about 1.75 times
# as long as one alone. WORKERS=0 runs everything in one process, in order.
WORKERS ?= 7

test-integration: ## Run the tests against the reference stack (start it with ref-up)
	$(if $(filter-out naive hardened,$(EDITION)),$(error EDITION must be naive or hardened, not '$(EDITION)'))
	FAULTLINE_REQUIRE_STACK=1 uv run pytest -n $(WORKERS) --dist loadscope --no-loadscope-reorder \
		-m "$(if $(EDITION),$(INTEGRATION_MARKS_$(EDITION)),$(INTEGRATION_MARKS))"

check: lint test ## Lint and Docker-free tests, what CI runs on every OS

# Credentials live in reference/.env (gitignored). Created from the example on
# first use and never overwritten, so local edits survive.
reference/.env:
	cp reference/.env.example $@

# CI builds the platform image beforehand with a layer cache and starts the
# stack with REF_UP_FLAGS= so compose does not build it a second time.
REF_UP_FLAGS ?= --build

ref-up: reference/.env ## Build and start the reference stack, wait until it is healthy
	$(COMPOSE) up -d $(REF_UP_FLAGS) --wait

ref-down: reference/.env ## Stop the reference stack and delete its volumes
	$(COMPOSE) down -v

ref-ps: reference/.env ## Show the reference stack services
	$(COMPOSE) ps -a

ref-logs: reference/.env ## Follow the reference stack logs
	$(COMPOSE) logs -f --tail 100

clean: ## Remove caches, build output and faultline workspaces
	rm -rf .pytest_cache .ruff_cache dist build .faultline
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} +
