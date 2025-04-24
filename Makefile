.DEFAULT_GOAL := help
py_version ?= 3.12
PYTHON = python${py_version}
PACKAGE = aiocache_dynamodb

.PHONY: help
help:  ## Shows this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target> <arg=value>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m  %s\033[0m\n\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ 🛠  Testing and development
.PHONY: install
install: ## Installs package with development dependencies
	uv sync --locked --all-extras --dev --python=$(py_version)

.PHONY: badge
badge:
	genbadge coverage -i coverage.xml

.PHONY: run-tests
run-tests:
	uv run pytest --cov=$(PACKAGE) --cov-report term-missing --cov-fail-under=95 --cov-report xml:coverage.xml

.PHONY: test
test: localstack-init run-tests localstack-stop badge ## Run testing and coverage.

.PHONY: test-ci
test: localstack-init run-tests localstack-stop ## Run testing and coverage.

.PHONY: localstack-init
localstack-init: ## Starts localstack with init script
	localstack start -d --no-banner; localstack wait -t 45

.PHONY: localstack-stop
localstack-stop: ## Starts localstack with init script
	localstack stop

##@ 👷 Quality
.PHONY: ruff-check
ruff-check: ## Runs ruff without fixing issues
	uv run -m ruff check

.PHONY: ruff-format
ruff-format: ## Runs style checkers fixing issues
	uv run -m ruff format; uv run -m ruff check --fix

.PHONY: typing
typing: ## Runs pyright static type checking
	uv run -m pyright $(PACKAGE)/

.PHONY: check
check: ruff-check typing ## Runs all quality checks without fixing issues

.PHONY: style
style: ruff-format
