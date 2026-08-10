# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.2.0] - 2026-08-10

### Added

- `add_producer()`/`add_consumer()`/`get_producer()`/`get_consumer()` — register and look up additional named producers/consumers per app, independent of the default pair.
- A small exception hierarchy (`FlaskConfluentKafkaError` and subclasses `NotInitializedError`, `AlreadyRegisteredError`, `NotRegisteredError`, `ClientCreationError`, `ProduceError`, `ConsumeError`) in place of bare `RuntimeError`, so callers can distinguish failure modes without matching on message text. Every subclass still extends `RuntimeError`, so existing `except RuntimeError` code keeps working unchanged.
- A `py.typed` marker (PEP 561), so type checkers pick up this package's type hints in downstream projects.
- `mypy` in strict mode, enforced in CI alongside the existing lint/test checks.
- Support for the Flask application-factory pattern and for sharing a single `FlaskConfluentKafka()` instance across multiple apps.

### Changed

- `produce()` no longer blocks on `flush()` — it queues the message and polls once, non-blocking. Call `flush()` on `app.extensions["kafka_producer"]` directly for a synchronous delivery guarantee.
- `consume()` only raises for a *fatal* consumer error (`KafkaError.fatal()` is `True`); non-fatal errors (e.g. `KafkaError._PARTITION_EOF`) are now logged and treated as "no message this poll", returning `None` instead of raising.
- `sasl.*` config keys are only set for SASL protocols, instead of always being present (possibly empty) in the client config.

### Fixed

- `consume()` no longer resubscribes to the same topic list on every call, avoiding an unnecessary consumer-group rebalance on each poll of a `while True: consume(...)` loop.
- `consume()` no longer crashes on a message with no value (e.g. a tombstone in a compacted topic) — it now returns `None` for that case as well.
- Added a shutdown teardown hook so producers/consumers are flushed/closed on process exit.

## [0.1.1] and earlier

Released before this changelog existed; not documented here.

[Unreleased]: https://github.com/Riverfount/flask-confluent-kafka/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Riverfount/flask-confluent-kafka/releases/tag/v0.2.0