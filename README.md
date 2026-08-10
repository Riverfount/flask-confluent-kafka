# flask-confluent-kafka

[![PyPI](https://img.shields.io/pypi/v/flask-confluent-kafka.svg)](https://pypi.org/project/flask-confluent-kafka/)
[![Python versions](https://img.shields.io/pypi/pyversions/flask-confluent-kafka.svg)](https://pypi.org/project/flask-confluent-kafka/)
[![CI](https://github.com/Riverfount/flask-confluent-kafka/actions/workflows/gh.yml/badge.svg)](https://github.com/Riverfount/flask-confluent-kafka/actions/workflows/gh.yml)
[![License: GPL-3.0](https://img.shields.io/pypi/l/flask-confluent-kafka.svg)](#license)

A simple Flask extension for integrating [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) producers and consumers into a Flask application.

Full documentation: <https://riverfount.github.io/flask-confluent-kafka/>

## Features

- Configures a Kafka `Producer` and `Consumer` straight from your Flask app config.
- Optional SASL authentication support (`security.protocol` / `sasl.mechanism`).
- Small helper API to produce (`dict`/`str` payloads, auto-serialized) and consume messages without dealing with `confluent-kafka` directly.
- Register additional named producers/consumers (`add_producer()`/`add_consumer()`), independent of the default pair.
- Ships with a `py.typed` marker (PEP 561), so type checkers like mypy/pyright pick up the library's type hints in downstream projects.

## Installation

```bash
pip install flask-confluent-kafka
# or
uv add flask-confluent-kafka
```

Requires Python >= 3.13.

Also on PyPI: <https://pypi.org/project/flask-confluent-kafka/>

## Quickstart

```python
from flask import Flask
from flask_confluent_kafka import FlaskConfluentKafka

app = Flask(__name__)
app.config["KAFKA_SERVER"] = "localhost:9092"
app.config["KAFKA_GROUP_ID"] = "my-app-group"

kafka = FlaskConfluentKafka(app)

# Produce a message (dicts are JSON-encoded automatically)
kafka.produce("my-topic", {"hello": "world"})

# Consume a single message (returns None if nothing arrives within `timeout`)
message = kafka.consume(["my-topic"])
if message is not None:
    print(message)
```

The application factory pattern is also supported:

```python
kafka = FlaskConfluentKafka()

def create_app():
    app = Flask(__name__)
    kafka.init_app(app)
    return app
```

### Multiple producers/consumers

Register additional named producers/consumers, independent of the default pair:

```python
orders_producer = kafka.add_producer("orders")
orders_consumer = kafka.add_consumer("orders", group_id="orders-processor")

orders_producer.produce("orders", value=b'{"id": 1}')
orders_producer.poll(0)

orders_consumer.subscribe(["orders"])
msg = orders_consumer.poll(1.0)
```

Fetch a previously registered client by name from anywhere else in the app with `kafka.get_producer("orders")` / `kafka.get_consumer("orders")`. These are plain `confluent_kafka.Producer`/`Consumer` objects, so unlike `produce()`/`consume()` there's no `dict`-to-JSON auto-serialization, subscribe-once tracking, or non-fatal-error handling — use the raw client API directly. See [API](#api) below for the full reference.

## Configuration

All configuration is read from `app.config` in `init_app`:

| Config key       | Default           | Maps to                | Description                          |
|------------------|--------------------|-------------------------|---------------------------------------|
| `KAFKA_SERVER`   | `localhost:9092`  | `bootstrap.servers`    | Comma-separated list of Kafka brokers |
| `KAFKA_USERNAME` | `""`               | `sasl.username` *      | SASL username                         |
| `KAFKA_PASSWORD` | `""`               | `sasl.password` *      | SASL password                         |
| `KAFKA_PROTOCOL` | `PLAINTEXT`        | `security.protocol`    | e.g. `PLAINTEXT`, `SASL_SSL`          |
| `KAFKA_MECHANISM`| `PLAIN`            | `sasl.mechanism` *     | e.g. `PLAIN`, `SCRAM-SHA-256`         |
| `KAFKA_GROUP_ID` | `default_group`    | `group.id` (consumer)  | Consumer group id                     |

\* `sasl.username`, `sasl.password`, and `sasl.mechanism` are only added to the client config when `KAFKA_PROTOCOL` is `SASL_PLAINTEXT` or `SASL_SSL` (matched case-insensitively). For `PLAINTEXT` (the default) or plain `SSL`, these three keys are left out of the config passed to `confluent_kafka.Producer`/`Consumer` entirely.

## API

### `FlaskConfluentKafka(app: Flask | None = None)`

Creates the extension. If `app` is given, calls `init_app(app)` immediately; otherwise, call `init_app(app)` yourself later (application factory pattern).

### `init_app(app: Flask) -> None`

Reads the config keys above, creates a `confluent_kafka.Producer` and `confluent_kafka.Consumer` — the **default pair** — and stores them in `app.extensions["kafka_producer"]` / `app.extensions["kafka_consumer"]`. For additional producers/consumers, see `add_producer()`/`add_consumer()` below.

`produce()`/`consume()` resolve their client via Flask's `current_app` whenever an app context is active, falling back to the app passed to the constructor otherwise. This makes it safe to share a single `FlaskConfluentKafka()` instance across multiple apps — calls made under a given app's context always use that app's producer/consumer.

### `produce(topic: str, value: dict | str, key: str | None = None) -> None`

Queues a message for asynchronous delivery to `topic` (`dict` values are JSON-serialized; `str` values are UTF-8 encoded) and polls once, non-blocking, to serve any already-completed delivery callbacks. This doesn't wait for broker acknowledgment — call `flush()` on `app.extensions["kafka_producer"]` directly for a synchronous delivery guarantee. Raises `NotInitializedError` if the producer isn't initialized, or `ProduceError` if `produce()` itself fails (e.g. local queue full); broker-side delivery failures aren't surfaced here.

### `consume(topics: list[str], timeout: float = 1.0) -> str | None`

Polls for a single message on `topics`, returning its decoded value, or `None` if nothing arrived within `timeout` seconds, if a non-fatal consumer error occurred (e.g. `KafkaError._PARTITION_EOF`, or any other error librdkafka doesn't flag as fatal; logged as a warning), or if the message itself has no value (e.g. a tombstone in a compacted topic). Raises `ConsumeError` only for a fatal consumer error (`KafkaError.fatal()` is `True`), since that signals the client itself is broken and can't recover. Subscribes the consumer to `topics` the first time it's called (or whenever the requested topic set changes), not on every call — so a `while True: consume(...)` loop doesn't trigger a consumer-group rebalance on each poll.

### `add_producer(name: str, config_overrides: dict[str, Any] | None = None) -> Producer`

Creates and registers an additional named `confluent_kafka.Producer`, independent of the default one created by `init_app()`. Uses the same connection config `init_app()` builds from `app.config` (`bootstrap.servers`, `security.protocol`, and `sasl.*` when applicable), with `config_overrides` layered on top — so `config_overrides` can override anything, including `bootstrap.servers` itself, e.g. to point this producer at a different cluster. Stores the result in `app.extensions["kafka_producers"][name]` and registers its own `atexit` shutdown hook that flushes it on process exit. Meant to be called once per name at application setup time, not repeatedly (e.g. per-request) — each call registers its own `atexit` hook, so repeated calls with new names would accumulate them for the life of the process. Raises `NotInitializedError` if this instance hasn't been `init_app()`'d for the active app yet, `AlreadyRegisteredError` if `name` is already registered, or `ClientCreationError` if `Producer` construction itself fails.

### `add_consumer(name: str, *, group_id: str, config_overrides: dict[str, Any] | None = None) -> Consumer`

Creates and registers an additional named `confluent_kafka.Consumer`, independent of the default one created by `init_app()`. `group_id` is required — unlike the default consumer, it never falls back to `KAFKA_GROUP_ID`, since silently sharing a group id with another consumer would just make it join that consumer group and compete for partitions with it instead of running independently. Uses the same connection config as `add_producer()`, plus `group.id=group_id` and `auto.offset.reset="earliest"`, with `config_overrides` layered on top of all of that. Stores the result in `app.extensions["kafka_consumers"][name]` and registers its own `atexit` shutdown hook that closes it on process exit. Meant to be called once per name at application setup time, not repeatedly (e.g. per-request) — each call registers its own `atexit` hook, so repeated calls with new names would accumulate them for the life of the process. Raises the same exceptions as `add_producer()`, under the same conditions.

### `get_producer(name: str) -> Producer` / `get_consumer(name: str) -> Consumer`

Look up a producer/consumer previously registered with `add_producer()`/`add_consumer()` for the active app. Raises `NotRegisteredError` if none is registered under `name`.

## Exceptions

Every error raised by this extension is an instance of `FlaskConfluentKafkaError`, which also extends `RuntimeError` — existing `except RuntimeError` code keeps working unchanged.

- `NotInitializedError` — raised by `produce()`, `consume()`, `add_producer()`, and `add_consumer()` when called before `init_app()` for the active app.
- `AlreadyRegisteredError` — raised by `add_producer()`/`add_consumer()` for a duplicate name.
- `NotRegisteredError` — raised by `get_producer()`/`get_consumer()` for an unknown name.
- `ClientCreationError` — raised when constructing the underlying `Producer`/`Consumer` fails.
- `ProduceError` — raised when `produce()` itself fails (e.g. local queue full).
- `ConsumeError` — raised when `consume()` encounters a fatal consumer error.

## Shutdown

`init_app()` registers an [`atexit`](https://docs.python.org/3/library/atexit.html) hook per app that flushes its producer (bounded by a 10 second timeout) and closes its consumer when the Python process exits. This is intentionally not wired to Flask's `app.teardown_appcontext()` — that fires after every request, not at process shutdown, which would tear down the producer/consumer after the very first request instead of once at the end.

`add_producer()`/`add_consumer()` each register their own `atexit` hook the same way, scoped to just the one client they created.

## Known limitations

This project is early-stage. See the [issue tracker](https://github.com/Riverfount/flask-confluent-kafka/issues) for known bugs and planned improvements.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

GPL-3.0 — see the `license` field in `pyproject.toml`.
