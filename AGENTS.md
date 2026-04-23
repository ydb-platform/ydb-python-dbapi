# AGENTS.md

## Scope

This repository contains a Python DB-API 2.0 wrapper over the YDB Python SDK.
The main package lives in `ydb_dbapi/`, and the test suite in `tests/`.

This file is for coding agents working in this repository. Prefer concrete,
repo-specific guidance over generic Python advice.

## Tooling

- Use `poetry` for Python commands.
- Install dependencies with `poetry install`.
- Run Python tools as `poetry run ...`.
- Prefer existing Poe tasks from `pyproject.toml` when they fit.
- Do not add new dependencies unless explicitly requested by a human.

Common commands:

- `poetry run poe lint`: run the main lint gate used in CI
  (`mypy ydb_dbapi` and `ruff check`).
- `poetry run poe format-check`: verify formatting without modifying files.
- `poetry run poe format`: apply formatting with Ruff formatter.
- `poetry run poe tests`: run the full test suite.
- `poetry run pytest tests/test_convert_parameters.py`: run focused tests for
  query-parameter conversion logic.
- `poetry run pytest tests/test_connections.py -k pyformat`: run integration
  coverage for pyformat-related connection and cursor behavior.
- `poetry run pre-commit run -a`: run the repository hooks on all files.

## Repository map

- `ydb_dbapi/connections.py`: sync and async connection classes, connection
  factory functions, isolation level handling, session pool ownership.
- `ydb_dbapi/cursors.py`: sync and async cursor execution paths, buffering,
  transactional execution, parameter conversion hook for `pyformat`.
- `ydb_dbapi/utils.py`: error translation, credential preparation, and query
  parameter conversion logic.
- `ydb_dbapi/errors.py`: DB-API error hierarchy.
- `tests/test_connections.py`: end-to-end sync and async connection tests.
- `tests/test_cursors.py`: cursor behavior tests.
- `tests/test_convert_parameters.py`: focused coverage for `pyformat`
  placeholder conversion and type inference.
- `.github/docker/docker-compose.yml`: local YDB for integration tests.

## Testing expectations

- `poetry run poe lint` is the main lint gate used in CI.
- `poetry run poe lint` currently runs `mypy ydb_dbapi` and `ruff check`.
- Keep `ruff` happy at line length `79`.
- Any change must leave linters green.
- Any behavior change should come with tests.
- After making changes, run the relevant tests, not just static checks.
- Start with the smallest relevant test subset, then broader checks if needed.

Integration test notes:

- Many tests require a local YDB instance on `localhost:2136`.
- CI starts it from `.github/docker/docker-compose.yml`.
- Locally, bring it up before integration tests with:
  `docker compose -f .github/docker/docker-compose.yml up -d`

## Working rules for changes

- Keep sync and async APIs aligned. If behavior changes in `Connection` or
  `Cursor`, check the corresponding async implementation as well.
- Any feature or fix should usually exist in both sync and async forms.
- Do not edit `dist/` by hand.
- Avoid touching `poetry.lock` unless dependencies actually change.
- User-facing behavior changes should update `README.md`.
- Prefer targeted tests for the edited area and mention if integration tests
  could not run because YDB was unavailable.

Required validation before finishing:

- run `poetry run poe lint`;
- run focused tests for the changed area;
- if the change affects runtime DB behavior, run the relevant integration
  tests when YDB is available;
- if a check could not be run, say so explicitly.

## Architecture

The repository is small. Agents should read the relevant source files directly
instead of guessing.

- `ydb_dbapi/__init__.py`: public package exports and DB-API metadata.
- `ydb_dbapi/connections.py`: sync/async connection lifecycle, connection
  factory functions, session pools, isolation levels, and transaction setup.
- `ydb_dbapi/cursors.py`: sync/async query execution, buffering, result-set
  handling, and cursor state management.
- `ydb_dbapi/utils.py`: shared helpers such as error translation, credentials,
  trace handling, and query-parameter conversion.
- `ydb_dbapi/errors.py`: DB-API-compatible exception hierarchy.
- `ydb_dbapi/constants.py`: exported constants used by the DB-API surface.
- `tests/test_connections.py`: end-to-end connection and transaction behavior.
- `tests/test_cursors.py`: cursor fetch/state behavior.
- `tests/test_convert_parameters.py`: focused tests for pyformat conversion.

## Behavior changes

- If you change user-visible behavior, update both implementation and tests.
- If behavior exists in both sync and async APIs, keep both paths aligned.
- Update focused tests first, then integration coverage where relevant.
- Update `README.md` when the public API, supported behavior, or examples
  change.
- For query-parameter behavior, the main lookup points are
  `ydb_dbapi/utils.py`, `ydb_dbapi/cursors.py`,
  `tests/test_convert_parameters.py`, and `tests/test_connections.py`.

## Style notes

- The codebase prefers explicit imports over grouped imports.
- Keep changes small and local; this repository is compact enough that broad
  refactors are rarely necessary.
- Follow the existing exception style and DB-API error mapping instead of
  introducing new error shapes casually.
