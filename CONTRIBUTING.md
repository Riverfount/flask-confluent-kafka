# Contributing to flask-confluent-kafka

Thanks for considering contributing! This project is a small Flask extension wrapping `confluent-kafka`, and contributions of all sizes are welcome — bug fixes, tests, docs, new features.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone git@github.com:Riverfount/flask-confluent-kafka.git
cd flask-confluent-kafka
uv sync
```

This installs the project along with its dev dependencies (currently `pytest`, `pytest-cov`, and `ruff`) into a local `.venv`.

## Linting

```bash
uv run ruff check .
```

Auto-fixable issues can be fixed with:

```bash
uv run ruff check --fix .
```

## Documentation site

Docs are built with [MkDocs](https://www.mkdocs.org/) and the [Material](https://squidfunk.github.io/mkdocs-material/) theme, from the `docs/` folder.

```bash
uv sync --group docs
uv run mkdocs serve
```

Then open http://127.0.0.1:8000. The site is deployed to GitHub Pages automatically on push to `main`.

## Running tests

```bash
uv run pytest -v
```

CI runs the full coverage-gated suite on every pull request against `main`:

```bash
uv run pytest --cov=flask_confluent_kafka --cov-report=term-missing --cov-fail-under=90
```

`main` is protected — both `ruff check` and the coverage-gated test suite must pass before a pull request can be merged, including for repo admins.

## Making changes

1. Check the [issue tracker](https://github.com/Riverfount/flask-confluent-kafka/issues) for existing issues related to what you want to work on, or open a new one for anything not tracked yet.
2. Create a branch off `main`.
3. Make your changes, keeping them focused on the issue at hand.
4. Make sure `ruff check .` and `uv run pytest` pass.
5. Open a pull request referencing the issue it closes (e.g. `Closes #1`).

## Commit messages

Keep commit messages short and focused on *why* the change was made, not just *what* changed.

## Code style

- Type hints are used throughout; please keep new code typed.
- Avoid inline comments unless something is genuinely non-obvious (a workaround, a subtle invariant) — well-named code should speak for itself.

## Reporting bugs

Please open an issue with:

- What you expected to happen.
- What actually happened.
- Minimal steps/code to reproduce.
- Your Python, `confluent-kafka`, and `flask-confluent-kafka` versions.
