# flask-confluent-kafka

A simple Flask extension for integrating [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) producers and consumers into a Flask application.

Full documentation: <https://riverfount.github.io/flask-confluent-kafka/>

## Features

- Configures a Kafka `Producer` and `Consumer` straight from your Flask app config.
- Optional SASL authentication support (`security.protocol` / `sasl.mechanism`).
- Small helper API to produce (`dict`/`str` payloads, auto-serialized) and consume messages without dealing with `confluent-kafka` directly.

## Installation

```bash
pip install flask-confluent-kafka
# or
uv add flask-confluent-kafka
```

Requires Python >= 3.13.

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

## Configuration

All configuration is read from `app.config` in `init_app`:

| Config key       | Default           | Maps to                | Description                          |
|------------------|--------------------|-------------------------|---------------------------------------|
| `KAFKA_SERVER`   | `localhost:9092`  | `bootstrap.servers`    | Comma-separated list of Kafka brokers |
| `KAFKA_USERNAME` | `""`               | `sasl.username`        | SASL username                         |
| `KAFKA_PASSWORD` | `""`               | `sasl.password`        | SASL password                         |
| `KAFKA_PROTOCOL` | `PLAINTEXT`        | `security.protocol`    | e.g. `PLAINTEXT`, `SASL_SSL`          |
| `KAFKA_MECHANISM`| `PLAIN`            | `sasl.mechanism`       | e.g. `PLAIN`, `SCRAM-SHA-256`         |
| `KAFKA_GROUP_ID` | `default_group`    | `group.id` (consumer)  | Consumer group id                     |

## API

### `FlaskConfluentKafka(app=None)`

Creates the extension. If `app` is given, calls `init_app(app)` immediately; otherwise, call `init_app(app)` yourself later (application factory pattern).

### `init_app(app)`

Reads the config keys above, creates a `confluent_kafka.Producer` and `confluent_kafka.Consumer`, and stores them in `app.extensions["kafka_producer"]` / `app.extensions["kafka_consumer"]`.

`produce()`/`consume()` resolve their client via Flask's `current_app` whenever an app context is active, falling back to the app passed to the constructor otherwise. This makes it safe to share a single `FlaskConfluentKafka()` instance across multiple apps — calls made under a given app's context always use that app's producer/consumer.

### `produce(topic: str, value: dict | str, key: str | None = None) -> None`

Queues a message for asynchronous delivery to `topic` (`dict` values are JSON-serialized; `str` values are UTF-8 encoded) and polls once, non-blocking, to serve any already-completed delivery callbacks. This doesn't wait for broker acknowledgment — call `flush()` on `app.extensions["kafka_producer"]` directly for a synchronous delivery guarantee. Raises `RuntimeError` if the producer isn't initialized, or if `produce()` itself fails (e.g. local queue full); broker-side delivery failures aren't surfaced here.

### `consume(topics: list[str], timeout: float = 1.0) -> str | None`

Polls for a single message on `topics`, returning its decoded value, or `None` if nothing arrived within `timeout` seconds. Raises `RuntimeError` on a consumer error. Subscribes the consumer to `topics` the first time it's called (or whenever the requested topic set changes), not on every call — so a `while True: consume(...)` loop doesn't trigger a consumer-group rebalance on each poll.

## Shutdown

`init_app()` registers an [`atexit`](https://docs.python.org/3/library/atexit.html) hook per app that flushes its producer (bounded by a 10 second timeout) and closes its consumer when the Python process exits. This is intentionally not wired to Flask's `app.teardown_appcontext()` — that fires after every request, not at process shutdown, which would tear down the producer/consumer after the very first request instead of once at the end.

## Known limitations

This project is early-stage. See the [issue tracker](https://github.com/Riverfount/flask-confluent-kafka/issues) for known bugs and planned improvements — notably around support for multiple producers/consumers per app.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

GPL-3.0 — see the `license` field in `pyproject.toml`.
