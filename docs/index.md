# flask-confluent-kafka

A simple Flask extension for integrating [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) producers and consumers into a Flask application.

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

Next: [Quickstart](quickstart.md).

## Known limitations

This project is early-stage. See the [issue tracker](https://github.com/Riverfount/flask-confluent-kafka/issues) for known bugs and planned improvements — notably around application-factory support, consumer/producer lifecycle management, and support for multiple producers/consumers per app.

## License

GPL-3.0 — see the `license` field in `pyproject.toml`.
