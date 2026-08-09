# API Reference

## `FlaskConfluentKafka(app=None)`

Creates the extension. If `app` is given, calls `init_app(app)` immediately; otherwise, call `init_app(app)` yourself later (application factory pattern).

## `init_app(app)`

Reads the config keys described in [Configuration](configuration.md), creates a `confluent_kafka.Producer` and `confluent_kafka.Consumer`, and stores them in `app.extensions["kafka_producer"]` / `app.extensions["kafka_consumer"]`.

## Client resolution (multiple apps)

`produce()`/`consume()` don't just reuse whichever app was `init_app()`'d last. They resolve the producer/consumer in this order:

1. If a Flask application context is active (`flask.has_app_context()`), use `current_app.extensions`.
2. Otherwise, fall back to the app passed to the constructor (or the last one passed to `init_app()`, for the no-arg constructor case).
3. If neither yields a client, raise `RuntimeError`.

This means a single `FlaskConfluentKafka()` instance shared across multiple apps is safe as long as calls happen inside the relevant app's context (e.g. during a request, or inside `with app.app_context():`). The one edge case not covered: calling `produce()`/`consume()` with **no context active at all** when the instance has been `init_app()`'d against more than one app — the fallback then uses whichever app was initialized last.

## `produce(topic: str, value: dict | str, key: str | None = None) -> None`

Sends a message to `topic`. `dict` values are JSON-serialized; `str` values are UTF-8 encoded. Raises `RuntimeError` on failure.

## `consume(topics: list[str], timeout: float = 1.0) -> str | None`

Polls for a single message on `topics`, returning its decoded value, or `None` if nothing arrived within `timeout` seconds. Raises `RuntimeError` on a consumer error. Subscribes the consumer to `topics` the first time it's called (or whenever the requested topic set changes), not on every call — so a `while True: consume(...)` loop doesn't trigger a consumer-group rebalance on each poll.
