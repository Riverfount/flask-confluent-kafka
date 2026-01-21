from typing import Any

from confluent_kafka import Consumer, Producer
import json


class FlaskConfluentKafka:
    def __init__(self, app=None):
        self.app = app
        if self.app is not None:
            self.init_app(self.app)

    def init_app(self, app):
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
        except Exception as e:
            raise RuntimeError(f"Failed to create Kafka producer: {e}")

        # Initialize Kafka Consumer
        try:
            self.consumer = Consumer({**kafka_config, "group.id": self.group_id, "auto.offset.reset": "earliest"})
        except Exception as e:
            raise RuntimeError(f"Failed to create Kafka consumer: {e}")

        # Store the extension in the app's extensions dictionary
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions["kafka_producer"] = self.producer
        app.extensions["kafka_consumer"] = self.consumer

    def produce(self, topic: str, value: dict[str, Any] | str, key: str = None) -> None:
        """Send a message to a Kafka topic."""
        if self.producer is None:
            raise RuntimeError("Kafka producer is not initialized.")
        
        if isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, str):
            value = value.encode("utf-8")

        try:
            self.producer.produce(topic, value=value, key=key)
            self.producer.flush()
        except Exception as e:
            raise RuntimeError(f"Failed to produce message: {e}")

    def consume(self, topics: list[str], timeout=1.0) -> str | None:
        """Consume messages from Kafka topics."""
        if self.consumer is None:
            raise RuntimeError("Kafka consumer is not initialized.")
        self.consumer.subscribe(topics)
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise Exception(f"Consumer error: {msg.error()}")
        return msg.value().decode("utf-8")
