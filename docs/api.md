# API Reference

## `FlaskConfluentKafka(app=None)`

Creates the extension. If `app` is given, calls `init_app(app)` immediately; otherwise, call `init_app(app)` yourself later (application factory pattern).

## `init_app(app)`

Reads the config keys described in [Configuration](configuration.md), creates a `confluent_kafka.Producer` and `confluent_kafka.Consumer`, and stores them in `app.extensions["kafka_producer"]` / `app.extensions["kafka_consumer"]`.

## `produce(topic: str, value: dict | str, key: str | None = None) -> None`

Sends a message to `topic`. `dict` values are JSON-serialized; `str` values are UTF-8 encoded. Raises `RuntimeError` on failure.

## `consume(topics: list[str], timeout: float = 1.0) -> str | None`

Subscribes to `topics` and polls for a single message, returning its decoded value, or `None` if nothing arrived within `timeout` seconds. Raises `RuntimeError` on a consumer error.
