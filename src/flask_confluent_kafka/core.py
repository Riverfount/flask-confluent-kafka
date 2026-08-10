import atexit
import json
import logging
from typing import Any, cast

from confluent_kafka import Consumer, KafkaException, Producer
from flask import Flask, current_app, has_app_context

logger = logging.getLogger(__name__)

_DEFAULT_SHUTDOWN_FLUSH_TIMEOUT = 10.0


def _shutdown_kafka_clients(
    producer: Producer | None = None,
    consumer: Consumer | None = None,
    flush_timeout: float = _DEFAULT_SHUTDOWN_FLUSH_TIMEOUT,
) -> None:
    """Flush a producer and/or close a consumer on process shutdown.

    Takes the clients as plain arguments (bound via atexit.register's own
    *args at registration time) rather than reading self.producer/consumer,
    so each hook keeps acting on the specific client(s) it was registered
    for. producer/consumer are independently optional so one function backs
    three registration paths: init_app() passes both (the default pair),
    while add_producer()/add_consumer() each pass only the one client they
    created, leaving the other None.
    """
    try:
        if producer is not None:
            producer.flush(flush_timeout)
    finally:
        if consumer is not None:
            consumer.close()


class FlaskConfluentKafka:
    def __init__(self, app: Flask | None = None) -> None:
        self.app = app
        self.producer: Producer | None = None
        self.consumer: Consumer | None = None
        if self.app is not None:
            self.init_app(self.app)

    def init_app(self, app: Flask) -> None:
        self.app = app
        group_id = app.config.get("KAFKA_GROUP_ID", "default_group")
        kafka_config = self._build_kafka_config(app)

        # Initialize Kafka Producer
        try:
            self.producer = Producer(kafka_config)
        except KafkaException as e:
            raise RuntimeError(f"Failed to create Kafka producer: {e}")

        # Initialize Kafka Consumer
        try:
            self.consumer = Consumer({**kafka_config, "group.id": group_id, "auto.offset.reset": "earliest"})
        except KafkaException as e:
            raise RuntimeError(f"Failed to create Kafka consumer: {e}")

        # Store the extension in the app's extensions dictionary
        if not hasattr(app, "extensions"):
            app.extensions = {}  # pragma: no cover
        app.extensions["kafka_producer"] = self.producer
        app.extensions["kafka_consumer"] = self.consumer

        atexit.register(_shutdown_kafka_clients, self.producer, self.consumer)

    def _build_kafka_config(self, app: Flask) -> dict[str, Any]:
        """Build the base client config (bootstrap.servers, security.protocol,
        and conditionally sasl.*) from app.config.

        Takes app explicitly rather than reading self, so it's correct for
        whichever app is passed in -- reused by init_app() and by
        add_producer()/add_consumer() for whatever app _resolve_app()
        currently yields, not necessarily the one init_app() was last
        called for.
        """
        protocol = app.config.get("KAFKA_PROTOCOL", "PLAINTEXT")
        kafka_config: dict[str, Any] = {
            "bootstrap.servers": app.config.get("KAFKA_SERVER", "localhost:9092"),
            "security.protocol": protocol,
        }

        if protocol.upper().startswith("SASL_"):
            kafka_config["sasl.username"] = app.config.get("KAFKA_USERNAME", "")
            kafka_config["sasl.password"] = app.config.get("KAFKA_PASSWORD", "")
            kafka_config["sasl.mechanism"] = app.config.get("KAFKA_MECHANISM", "PLAIN")

        return kafka_config

    def _resolve_app(self) -> Flask | None:
        """Resolve the active app: current_app takes priority over the
        constructor's app so calls made while a given app's context is
        active never leak another app's state, when this instance is
        shared across multiple apps.
        """
        if has_app_context():
            return current_app
        return self.app

    def _resolve_initialized_app(self) -> Flask:
        """Resolve the active app (see _resolve_app) and confirm it's
        already been init_app()'d.

        add_producer()/add_consumer() register additional named clients for
        an app already using this extension -- they don't stand in for
        init_app(). Also rejects an active context for some other,
        unrelated Flask app that was never init_app()'d with this instance.
        """
        app = self._resolve_app()
        if app is None or "kafka_producer" not in app.extensions:
            raise RuntimeError("Kafka extension is not initialized for this app; call init_app() first.")
        return app

    def _get_client(self, extension_key: str, label: str, name: str | None = None) -> Producer | Consumer:
        """Resolve the default (name=None) or a named producer/consumer for the active app (see `_resolve_app`)."""
        app = self._resolve_app()

        if name is None:
            client: Producer | Consumer | None = app.extensions.get(extension_key) if app is not None else None
            if client is None:
                raise RuntimeError(f"Kafka {label} is not initialized.")
            return client

        registry: dict[str, Producer | Consumer] | None = app.extensions.get(extension_key) if app is not None else None
        client = registry.get(name) if registry is not None else None
        if client is None:
            raise RuntimeError(f"Kafka {label} '{name}' is not registered for this app.")
        return client

    def _register_client(
        self,
        name: str,
        *,
        registry_key: str,
        label: str,
        client_cls: type[Producer] | type[Consumer],
        extra_config: dict[str, Any] | None = None,
        config_overrides: dict[str, Any] | None = None,
    ) -> Producer | Consumer:
        """Shared skeleton for add_producer()/add_consumer(): resolve the
        initialized app, reject a duplicate name, build config, construct
        the client, and store it in the named registry.

        Caller is responsible for the atexit shutdown hook, since a
        producer/consumer occupies a different positional slot in
        _shutdown_kafka_clients.
        """
        app = self._resolve_initialized_app()
        registry: dict[str, Producer | Consumer] = app.extensions.setdefault(registry_key, {})

        if name in registry:
            raise RuntimeError(f"{label} '{name}' is already registered for this app.")

        client_config = {**self._build_kafka_config(app), **(extra_config or {}), **(config_overrides or {})}
        try:
            client = client_cls(client_config)
        except KafkaException as e:
            raise RuntimeError(f"Failed to create Kafka {label.lower()} '{name}': {e}")

        registry[name] = client
        return client

    def add_producer(self, name: str, config_overrides: dict[str, Any] | None = None) -> Producer:
        """Create and register an additional named Producer for the active
        app, independent of the default one created by init_app().

        Uses the same config init_app() builds from app.config, with
        config_overrides layered on top (can override anything, including
        bootstrap.servers, e.g. to point this producer at a different
        cluster). Stores the result in app.extensions["kafka_producers"][name]
        and registers its own atexit shutdown hook that flushes it.

        Meant to be called once per name at application setup time, not
        repeatedly (e.g. per-request) -- each call registers its own atexit
        hook, so repeated calls with new names would accumulate them for
        the life of the process.

        Raises RuntimeError if this app hasn't been init_app()'d yet, if
        `name` is already registered, or if Producer construction fails.
        """
        producer = cast(
            Producer,
            self._register_client(
                name,
                registry_key="kafka_producers",
                label="Producer",
                client_cls=Producer,
                config_overrides=config_overrides,
            ),
        )
        atexit.register(_shutdown_kafka_clients, producer, None)
        return producer

    def add_consumer(self, name: str, *, group_id: str, config_overrides: dict[str, Any] | None = None) -> Consumer:
        """Create and register an additional named Consumer for the active
        app, independent of the default one created by init_app().

        group_id is required, with no fallback to KAFKA_GROUP_ID -- a second
        consumer silently sharing the default group id would just join that
        consumer group and compete for partitions with it, instead of
        running independently. Uses the same config as add_producer(), plus
        group.id=group_id and auto.offset.reset="earliest", with
        config_overrides layered on top of all of that. Stores the result
        in app.extensions["kafka_consumers"][name] and registers its own
        atexit shutdown hook that closes it.

        Meant to be called once per name at application setup time, not
        repeatedly (e.g. per-request) -- each call registers its own atexit
        hook, so repeated calls with new names would accumulate them for
        the life of the process.

        Raises RuntimeError under the same conditions as add_producer().
        """
        consumer = cast(
            Consumer,
            self._register_client(
                name,
                registry_key="kafka_consumers",
                label="Consumer",
                client_cls=Consumer,
                extra_config={"group.id": group_id, "auto.offset.reset": "earliest"},
                config_overrides=config_overrides,
            ),
        )
        atexit.register(_shutdown_kafka_clients, None, consumer)
        return consumer

    def get_producer(self, name: str) -> Producer:
        """Return the named Producer registered via add_producer()."""
        return cast(Producer, self._get_client("kafka_producers", "producer", name))

    def get_consumer(self, name: str) -> Consumer:
        """Return the named Consumer registered via add_consumer()."""
        return cast(Consumer, self._get_client("kafka_consumers", "consumer", name))

    def produce(self, topic: str, value: dict[str, Any] | str, key: str | None = None) -> None:
        """Send a message to a Kafka topic.

        Queues the message and polls once, non-blocking, to serve any
        already-completed delivery callbacks -- this does not wait for
        broker acknowledgment. BufferError/KafkaException raised by produce()
        itself (e.g. local queue full) still raise RuntimeError; broker-side
        delivery failures are not surfaced here. Call flush() on
        app.extensions["kafka_producer"] directly for a delivery guarantee.
        """
        producer = cast(Producer, self._get_client("kafka_producer", "producer"))

        payload: str | bytes = json.dumps(value) if isinstance(value, dict) else value.encode("utf-8")

        try:
            producer.produce(topic, value=payload, key=key)
            producer.poll(0)
        except (BufferError, KafkaException) as e:
            raise RuntimeError(f"Failed to produce message: {e}")

    def consume(self, topics: list[str], timeout: float = 1.0) -> str | None:
        """Consume messages from Kafka topics.

        Subscribes only when the requested topics differ from what this
        app's consumer is already subscribed to, so repeated calls in a
        consume loop don't trigger a rebalance on every poll.

        A msg.error() is only raised as a RuntimeError when librdkafka has
        flagged it fatal (error.fatal() is True) -- the client itself is
        broken and can't recover. Any other error -- including purely
        informational ones like KafkaError._PARTITION_EOF, and
        transient/retriable errors librdkafka can recover from on its own --
        is logged and treated as "no message this poll", returning None. A
        message with no value (e.g. a tombstone in a compacted topic) also
        returns None rather than raising.
        """
        consumer = cast(Consumer, self._get_client("kafka_consumer", "consumer"))
        app = self._resolve_initialized_app()

        requested_topics = frozenset(topics)
        if app.extensions.get("kafka_subscribed_topics") != requested_topics:
            consumer.subscribe(topics)
            app.extensions["kafka_subscribed_topics"] = requested_topics

        msg = consumer.poll(timeout)
        if msg is None:
            return None

        error = msg.error()
        if error is not None:
            if error.fatal():
                raise RuntimeError(f"Consumer error: {error}")
            logger.warning("Non-fatal Kafka consumer error: %s", error)
            return None

        raw_value = msg.value()
        if raw_value is None:
            return None
        return raw_value.decode("utf-8")
