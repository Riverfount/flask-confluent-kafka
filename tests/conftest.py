from unittest.mock import MagicMock

import pytest
from flask import Flask


@pytest.fixture
def mock_kafka_clients(monkeypatch):
    producer_cls = MagicMock(side_effect=lambda *a, **kw: MagicMock(name="producer"))
    consumer_cls = MagicMock(side_effect=lambda *a, **kw: MagicMock(name="consumer"))
    monkeypatch.setattr("flask_confluent_kafka.core.Producer", producer_cls)
    monkeypatch.setattr("flask_confluent_kafka.core.Consumer", consumer_cls)
    return producer_cls, consumer_cls


@pytest.fixture
def mock_atexit_register(monkeypatch):
    """Capture atexit.register(...) calls instead of actually registering them.

    core.py does `import atexit`, so `flask_confluent_kafka.core.atexit` is
    the same object as the real atexit module -- patching this attribute
    patches the real atexit.register for the test's duration.
    """
    registered = []

    def _register(func, *args, **kwargs):
        registered.append((func, args, kwargs))
        return func

    monkeypatch.setattr("flask_confluent_kafka.core.atexit.register", _register)
    return registered


@pytest.fixture
def make_app(mock_kafka_clients):
    def _make_app(name: str = "test-app") -> Flask:
        app = Flask(name)
        app.config.update(KAFKA_SERVER="localhost:9092", KAFKA_GROUP_ID="test-group")
        return app

    return _make_app


@pytest.fixture
def app(make_app) -> Flask:
    return make_app()
