import json
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from flask import current_app, has_app_context


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

    def _get_client(self, extension_key: str) -> Producer | Consumer:
        """Resolve the producer/consumer for the active app.

        current_app takes priority over the constructor's app so calls made
        while a given app's context is active never leak another app's
        client, when this instance is shared across multiple apps.
        """
        if has_app_context():
            client = current_app.extensions.get(extension_key)
        elif self.app is not None:
            client = self.app.extensions.get(extension_key)
        else:
            client = None

        if client is None:
            label = extension_key.removeprefix("kafka_")
            raise RuntimeError(f"Kafka {label} is not initialized.")
        return client

    def produce(self, topic: str, value: dict[str, Any] | str, key: str | None = None) -> None:
        """Send a message to a Kafka topic."""
        producer = self._get_client("kafka_producer")

        if isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, str):
            value = value.encode("utf-8")

        try:
            producer.produce(topic, value=value, key=key)
            producer.flush()
        except (BufferError, KafkaException) as e:
            raise RuntimeError(f"Failed to produce message: {e}")

    def consume(self, topics: list[str], timeout=1.0) -> str | None:
        """Consume messages from Kafka topics."""
        consumer = self._get_client("kafka_consumer")
        consumer.subscribe(topics)
        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Consumer error: {msg.error()}")
        return msg.value().decode("utf-8")
