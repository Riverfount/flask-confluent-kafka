import atexit
import json
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from flask import current_app, has_app_context

_DEFAULT_SHUTDOWN_FLUSH_TIMEOUT = 10.0


def _shutdown_kafka_clients(
    producer: Producer, consumer: Consumer, flush_timeout: float = _DEFAULT_SHUTDOWN_FLUSH_TIMEOUT
) -> None:
    """Flush a producer and close a consumer on process shutdown.

    Takes the clients as plain arguments (bound via atexit.register's own
    *args at registration time) rather than reading self.producer/consumer,
    so each app's hook keeps closing that app's own clients even after a
    later init_app() call for a different app overwrites self's attributes.
    """
    try:
        producer.flush(flush_timeout)
    finally:
        consumer.close()


class FlaskConfluentKafka:
    def __init__(self, app=None):
        self.app = app
        self.producer = None
        self.consumer = None
        if self.app is not None:
            self.init_app(self.app)

    def init_app(self, app):
        self.app = app
        self.bootstrap_servers = app.config.get("KAFKA_SERVER", "localhost:9092")
        self.username = app.config.get("KAFKA_USERNAME", "")
        self.password = app.config.get("KAFKA_PASSWORD", "")
        self.protocol = app.config.get("KAFKA_PROTOCOL", "PLAINTEXT")
        self.mechanism = app.config.get("KAFKA_MECHANISM", "PLAIN")
        self.group_id = app.config.get("KAFKA_GROUP_ID", "default_group")

        # Set up Kafka Server configuration
        kafka_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "sasl.username": self.username,
            "sasl.password": self.password,
            "security.protocol": self.protocol,
            "sasl.mechanism": self.mechanism,
        }

        # Initialize Kafka Producer
        try:
            self.producer = Producer(kafka_config)
        except KafkaException as e:
            raise RuntimeError(f"Failed to create Kafka producer: {e}")

        # Initialize Kafka Consumer
        try:
            self.consumer = Consumer({**kafka_config, "group.id": self.group_id, "auto.offset.reset": "earliest"})
        except KafkaException as e:
            raise RuntimeError(f"Failed to create Kafka consumer: {e}")

        # Store the extension in the app's extensions dictionary
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions["kafka_producer"] = self.producer
        app.extensions["kafka_consumer"] = self.consumer

        atexit.register(_shutdown_kafka_clients, self.producer, self.consumer)

    def _resolve_app(self):
        """Resolve the active app: current_app takes priority over the
        constructor's app so calls made while a given app's context is
        active never leak another app's state, when this instance is
        shared across multiple apps.
        """
        if has_app_context():
            return current_app
        return self.app

    def _get_client(self, extension_key: str) -> Producer | Consumer:
        """Resolve the producer/consumer for the active app (see `_resolve_app`)."""
        app = self._resolve_app()
        client = app.extensions.get(extension_key) if app is not None else None

        if client is None:
            label = extension_key.removeprefix("kafka_")
            raise RuntimeError(f"Kafka {label} is not initialized.")
        return client

    def produce(self, topic: str, value: dict[str, Any] | str, key: str | None = None) -> None:
        """Send a message to a Kafka topic.

        Queues the message and polls once, non-blocking, to serve any
        already-completed delivery callbacks -- this does not wait for
        broker acknowledgment. BufferError/KafkaException raised by produce()
        itself (e.g. local queue full) still raise RuntimeError; broker-side
        delivery failures are not surfaced here. Call flush() on
        app.extensions["kafka_producer"] directly for a delivery guarantee.
        """
        producer = self._get_client("kafka_producer")

        if isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, str):
            value = value.encode("utf-8")

        try:
            producer.produce(topic, value=value, key=key)
            producer.poll(0)
        except (BufferError, KafkaException) as e:
            raise RuntimeError(f"Failed to produce message: {e}")

    def consume(self, topics: list[str], timeout=1.0) -> str | None:
        """Consume messages from Kafka topics.

        Subscribes only when the requested topics differ from what this
        app's consumer is already subscribed to, so repeated calls in a
        consume loop don't trigger a rebalance on every poll.
        """
        consumer = self._get_client("kafka_consumer")
        app = self._resolve_app()

        requested_topics = frozenset(topics)
        if app.extensions.get("kafka_subscribed_topics") != requested_topics:
            consumer.subscribe(topics)
            app.extensions["kafka_subscribed_topics"] = requested_topics

        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Consumer error: {msg.error()}")
        return msg.value().decode("utf-8")
