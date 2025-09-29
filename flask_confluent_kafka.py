from confluent_kafka import Consumer, Producer


class FlaskConfluentKafka:
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.bootstrap_servers = app.config.get("KAFKA_SERVER", "localhost:9092")
        self.username = app.config.get("KAFKA_USERNAME", "")
        self.password = app.config.get("KAFKA_PASSWORD", "")
        self.protocol = app.config.get("KAFKA_PROTOCOL", "PLAINTEXT")
        self.mechanism = app.config.get("KAFKA_MECHANISM", "PLAIN")
        self.group_id = app.config.get("KAFKA_GROUP_ID", "default_group")
        self.producer = None
        self.consumer = None
        # Set up Kafka Server configuration
        kafka_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "sasl.username": self.username,
            "sasl.password": self.password,
            "security.protocol": self.protocol,
            "sasl.mechanism": self.mechanism,
        }

        # Initialize Kafka Producer
        self.producer = Producer(kafka_config)

        # Initialize Kafka Consumer
        self.consumer = Consumer({**kafka_config, "group.id": self.group_id, "auto.offset.reset": "earliest"})

        # Store the extension in the app's extensions dictionary
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions["kafka_producer"] = self.producer
        app.extensions["kafka_consumer"] = self.consumer

    def send(self, topic, value, key=None):
        """Send a message to a Kafka topic."""
        if self.producer is None:
            raise RuntimeError("Kafka producer is not initialized.")
        self.producer.produce(topic, value=value, key=key)
        self.producer.flush()

    def consume(self, topics, timeout=1.0):
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
