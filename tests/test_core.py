import json
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

from flask_confluent_kafka import FlaskConfluentKafka


def test_produce_before_init_app_raises_runtime_error():
    kafka = FlaskConfluentKafka()
    with pytest.raises(RuntimeError, match="producer is not initialized"):
        kafka.produce("topic", "value")


def test_consume_before_init_app_raises_runtime_error():
    kafka = FlaskConfluentKafka()
    with pytest.raises(RuntimeError, match="consumer is not initialized"):
        kafka.consume(["topic"])


def test_produce_falls_back_to_constructor_app_without_a_context(app):
    kafka = FlaskConfluentKafka(app)
    kafka.produce("topic", "value")
    producer = app.extensions["kafka_producer"]
    producer.produce.assert_called_once()
    producer.poll.assert_called_once_with(0)


def test_produce_does_not_flush_synchronously(app):
    kafka = FlaskConfluentKafka(app)
    kafka.produce("topic", "value")
    app.extensions["kafka_producer"].flush.assert_not_called()


def test_produce_json_serializes_a_dict_value(app):
    kafka = FlaskConfluentKafka(app)

    kafka.produce("topic", {"hello": "world"})

    producer = app.extensions["kafka_producer"]
    assert producer.produce.call_args.kwargs["value"] == json.dumps({"hello": "world"})
    producer.poll.assert_called_once_with(0)


def test_produce_wraps_a_kafka_exception_in_a_runtime_error(app):
    kafka = FlaskConfluentKafka(app)
    producer = app.extensions["kafka_producer"]
    producer.produce.side_effect = KafkaException("boom")

    with pytest.raises(RuntimeError, match="Failed to produce message"):
        kafka.produce("topic", "value")

    producer.poll.assert_not_called()


def test_produce_wraps_a_buffer_error_in_a_runtime_error(app):
    kafka = FlaskConfluentKafka(app)
    producer = app.extensions["kafka_producer"]
    producer.produce.side_effect = BufferError("queue full")

    with pytest.raises(RuntimeError, match="Failed to produce message"):
        kafka.produce("topic", "value")

    producer.poll.assert_not_called()


def test_consume_falls_back_to_constructor_app_without_a_context(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    consumer.poll.return_value = None
    assert kafka.consume(["topic"]) is None
    consumer.poll.assert_called_once()


def test_produce_uses_current_app_when_a_context_is_active(app):
    kafka = FlaskConfluentKafka(app)
    with app.app_context():
        kafka.produce("topic", "value")
    app.extensions["kafka_producer"].produce.assert_called_once()


def test_produce_uses_the_correct_app_when_two_apps_share_one_extension(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    with app_a.app_context():
        kafka.produce("topic", "value")
    with app_b.app_context():
        kafka.produce("topic", "value")

    app_a.extensions["kafka_producer"].produce.assert_called_once()
    app_b.extensions["kafka_producer"].produce.assert_called_once()


def test_consume_uses_the_correct_app_when_two_apps_share_one_extension(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    msg_a, msg_b = MagicMock(), MagicMock()
    msg_a.error.return_value = None
    msg_a.value.return_value.decode.return_value = "from-a"
    msg_b.error.return_value = None
    msg_b.value.return_value.decode.return_value = "from-b"
    app_a.extensions["kafka_consumer"].poll.return_value = msg_a
    app_b.extensions["kafka_consumer"].poll.return_value = msg_b

    with app_a.app_context():
        assert kafka.consume(["topic"]) == "from-a"
    with app_b.app_context():
        assert kafka.consume(["topic"]) == "from-b"


def test_consume_only_subscribes_once_for_repeated_calls_with_the_same_topics(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    consumer.poll.return_value = None

    kafka.consume(["topic"])
    kafka.consume(["topic"])

    consumer.subscribe.assert_called_once_with(["topic"])


def test_consume_resubscribes_when_the_topic_list_changes(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    consumer.poll.return_value = None

    kafka.consume(["topic-a"])
    kafka.consume(["topic-b"])

    assert consumer.subscribe.call_count == 2
    consumer.subscribe.assert_called_with(["topic-b"])


def test_consume_does_not_resubscribe_when_only_topic_order_changes(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    consumer.poll.return_value = None

    kafka.consume(["a", "b"])
    kafka.consume(["b", "a"])

    consumer.subscribe.assert_called_once_with(["a", "b"])


def test_consume_tracks_subscriptions_independently_per_app(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    consumer_a = app_a.extensions["kafka_consumer"]
    consumer_b = app_b.extensions["kafka_consumer"]
    consumer_a.poll.return_value = None
    consumer_b.poll.return_value = None

    with app_a.app_context():
        kafka.consume(["topic"])
        kafka.consume(["topic"])

    with app_b.app_context():
        kafka.consume(["topic"])

    consumer_a.subscribe.assert_called_once_with(["topic"])
    consumer_b.subscribe.assert_called_once_with(["topic"])


def test_produce_raises_for_an_active_but_uninitialized_app(make_app):
    app_a, app_c = make_app("app-a"), make_app("app-c")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)  # only app_a is initialized

    with app_c.app_context(), pytest.raises(RuntimeError, match="producer is not initialized"):
        kafka.produce("topic", "value")


def test_init_app_registers_an_atexit_shutdown_hook(app, mock_atexit_register):
    FlaskConfluentKafka(app)

    assert len(mock_atexit_register) == 1
    _func, args, _kwargs = mock_atexit_register[0]
    assert args == (app.extensions["kafka_producer"], app.extensions["kafka_consumer"])


def test_init_app_registers_atexit_hook_bound_to_each_apps_own_clients(make_app, mock_atexit_register):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    assert len(mock_atexit_register) == 2
    _func_a, args_a, _kwargs_a = mock_atexit_register[0]
    _func_b, args_b, _kwargs_b = mock_atexit_register[1]
    assert args_a == (app_a.extensions["kafka_producer"], app_a.extensions["kafka_consumer"])
    assert args_b == (app_b.extensions["kafka_producer"], app_b.extensions["kafka_consumer"])
    assert args_a != args_b


def test_init_app_wraps_a_kafka_exception_in_a_runtime_error_for_the_producer(app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    producer_cls.side_effect = KafkaException("boom")

    with pytest.raises(RuntimeError, match="Failed to create Kafka producer"):
        FlaskConfluentKafka(app)


def test_init_app_wraps_a_kafka_exception_in_a_runtime_error_for_the_consumer(app, mock_kafka_clients):
    _producer_cls, consumer_cls = mock_kafka_clients
    consumer_cls.side_effect = KafkaException("boom")

    with pytest.raises(RuntimeError, match="Failed to create Kafka consumer"):
        FlaskConfluentKafka(app)


def test_atexit_shutdown_hook_flushes_producer_and_closes_consumer(app, mock_atexit_register):
    FlaskConfluentKafka(app)
    func, args, kwargs = mock_atexit_register[0]

    func(*args, **kwargs)

    app.extensions["kafka_producer"].flush.assert_called_once()
    app.extensions["kafka_consumer"].close.assert_called_once()


def test_atexit_shutdown_hook_still_closes_consumer_if_flush_raises(app, mock_atexit_register):
    FlaskConfluentKafka(app)
    func, args, kwargs = mock_atexit_register[0]
    producer, consumer = args
    producer.flush.side_effect = KafkaException("broker unreachable")

    with pytest.raises(KafkaException):
        func(*args, **kwargs)

    consumer.close.assert_called_once()


def test_consume_raises_for_a_fatal_consumer_error(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    msg = MagicMock()
    msg.error.return_value = KafkaError(KafkaError._ALL_BROKERS_DOWN, fatal=True)
    consumer.poll.return_value = msg

    with pytest.raises(RuntimeError, match="Consumer error"):
        kafka.consume(["topic"])


def test_consume_returns_none_for_a_non_fatal_consumer_error(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    msg = MagicMock()
    msg.error.return_value = KafkaError(KafkaError._PARTITION_EOF, fatal=False)
    consumer.poll.return_value = msg

    assert kafka.consume(["topic"]) is None


def test_consume_does_not_decode_a_message_with_a_non_fatal_error(app):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    msg = MagicMock()
    msg.error.return_value = KafkaError(KafkaError._PARTITION_EOF, fatal=False)
    consumer.poll.return_value = msg

    kafka.consume(["topic"])

    msg.value.assert_not_called()


def test_consume_logs_a_warning_for_a_non_fatal_consumer_error(app, caplog):
    kafka = FlaskConfluentKafka(app)
    consumer = app.extensions["kafka_consumer"]
    msg = MagicMock()
    msg.error.return_value = KafkaError(KafkaError._PARTITION_EOF, fatal=False)
    consumer.poll.return_value = msg

    with caplog.at_level("WARNING", logger="flask_confluent_kafka.core"):
        kafka.consume(["topic"])

    assert "Non-fatal Kafka consumer error" in caplog.text


def test_init_app_omits_sasl_keys_for_default_plaintext_protocol(app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    FlaskConfluentKafka(app)

    config = producer_cls.call_args.args[0]
    assert "sasl.username" not in config
    assert "sasl.password" not in config
    assert "sasl.mechanism" not in config
    assert config["security.protocol"] == "PLAINTEXT"


def test_init_app_omits_sasl_keys_for_plain_ssl_protocol(make_app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    app = make_app("app-ssl", KAFKA_PROTOCOL="SSL")
    FlaskConfluentKafka(app)

    config = producer_cls.call_args.args[0]
    assert "sasl.username" not in config
    assert "sasl.password" not in config
    assert "sasl.mechanism" not in config


def test_init_app_includes_sasl_keys_for_sasl_ssl_protocol(make_app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    app = make_app(
        "app-sasl-ssl",
        KAFKA_PROTOCOL="SASL_SSL",
        KAFKA_USERNAME="my-user",
        KAFKA_PASSWORD="my-password",
        KAFKA_MECHANISM="SCRAM-SHA-256",
    )
    FlaskConfluentKafka(app)

    config = producer_cls.call_args.args[0]
    assert config["sasl.username"] == "my-user"
    assert config["sasl.password"] == "my-password"
    assert config["sasl.mechanism"] == "SCRAM-SHA-256"
    assert config["security.protocol"] == "SASL_SSL"


def test_init_app_includes_sasl_keys_for_sasl_plaintext_protocol(make_app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    app = make_app("app-sasl-plaintext", KAFKA_PROTOCOL="SASL_PLAINTEXT", KAFKA_USERNAME="u", KAFKA_PASSWORD="p")
    FlaskConfluentKafka(app)

    config = producer_cls.call_args.args[0]
    assert "sasl.username" in config
    assert "sasl.password" in config
    assert "sasl.mechanism" in config


def test_init_app_matches_sasl_protocol_case_insensitively(make_app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    app = make_app("app-lowercase-protocol", KAFKA_PROTOCOL="sasl_ssl", KAFKA_USERNAME="u", KAFKA_PASSWORD="p")
    FlaskConfluentKafka(app)

    config = producer_cls.call_args.args[0]
    assert "sasl.username" in config
    assert "sasl.password" in config
    assert "sasl.mechanism" in config


def test_init_app_builds_the_consumer_config_with_the_same_sasl_key_omission(app, mock_kafka_clients):
    _producer_cls, consumer_cls = mock_kafka_clients
    FlaskConfluentKafka(app)

    config = consumer_cls.call_args.args[0]
    assert "sasl.username" not in config
    assert "sasl.password" not in config
    assert "sasl.mechanism" not in config
    assert config["group.id"] == "test-group"


def test_add_producer_registers_and_returns_the_producer(app):
    kafka = FlaskConfluentKafka(app)
    producer = kafka.add_producer("orders")
    assert producer is app.extensions["kafka_producers"]["orders"]


def test_add_producer_builds_config_from_app_config_plus_overrides(app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    kafka = FlaskConfluentKafka(app)

    kafka.add_producer("orders", config_overrides={"linger.ms": 5})

    config = producer_cls.call_args.args[0]
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["security.protocol"] == "PLAINTEXT"
    assert config["linger.ms"] == 5


def test_add_producer_raises_for_a_duplicate_name(app):
    kafka = FlaskConfluentKafka(app)
    kafka.add_producer("orders")

    with pytest.raises(RuntimeError, match="already registered"):
        kafka.add_producer("orders")


def test_add_producer_raises_when_the_instance_has_no_initialized_app():
    kafka = FlaskConfluentKafka()
    with pytest.raises(RuntimeError, match="not initialized"):
        kafka.add_producer("orders")


def test_add_producer_raises_for_an_active_but_uninitialized_app(make_app):
    app_a, app_c = make_app("app-a"), make_app("app-c")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)  # only app_a is initialized

    with app_c.app_context(), pytest.raises(RuntimeError, match="not initialized"):
        kafka.add_producer("orders")


def test_add_producer_wraps_a_kafka_exception_in_a_runtime_error(app, mock_kafka_clients):
    producer_cls, _consumer_cls = mock_kafka_clients
    kafka = FlaskConfluentKafka(app)
    producer_cls.side_effect = KafkaException("boom")

    with pytest.raises(RuntimeError, match="Failed to create Kafka producer"):
        kafka.add_producer("orders")


def test_add_producer_registers_an_atexit_hook_for_only_the_producer(app, mock_atexit_register):
    kafka = FlaskConfluentKafka(app)
    mock_atexit_register.clear()  # drop init_app's own hook; isolate add_producer's

    producer = kafka.add_producer("orders")

    assert len(mock_atexit_register) == 1
    _func, args, _kwargs = mock_atexit_register[0]
    assert args == (producer, None)


def test_add_producer_atexit_hook_flushes_without_touching_a_consumer(app, mock_atexit_register):
    kafka = FlaskConfluentKafka(app)
    mock_atexit_register.clear()
    producer = kafka.add_producer("orders")
    func, args, kwargs = mock_atexit_register[0]

    func(*args, **kwargs)

    producer.flush.assert_called_once()


def test_add_producer_registers_into_the_correct_apps_extensions_when_two_apps_share_one_extension(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    with app_a.app_context():
        producer_a = kafka.add_producer("orders")
    with app_b.app_context():
        producer_b = kafka.add_producer("orders")

    assert app_a.extensions["kafka_producers"]["orders"] is producer_a
    assert app_b.extensions["kafka_producers"]["orders"] is producer_b
    assert producer_a is not producer_b


def test_add_consumer_registers_and_returns_the_consumer(app):
    kafka = FlaskConfluentKafka(app)
    consumer = kafka.add_consumer("orders", group_id="orders-group")
    assert consumer is app.extensions["kafka_consumers"]["orders"]


def test_add_consumer_builds_config_with_the_given_group_id_and_overrides(app, mock_kafka_clients):
    _producer_cls, consumer_cls = mock_kafka_clients
    kafka = FlaskConfluentKafka(app)

    kafka.add_consumer("orders", group_id="orders-group", config_overrides={"auto.offset.reset": "latest"})

    config = consumer_cls.call_args.args[0]
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == "orders-group"
    assert config["auto.offset.reset"] == "latest"


def test_add_consumer_defaults_auto_offset_reset_to_earliest(app, mock_kafka_clients):
    _producer_cls, consumer_cls = mock_kafka_clients
    kafka = FlaskConfluentKafka(app)

    kafka.add_consumer("orders", group_id="orders-group")

    config = consumer_cls.call_args.args[0]
    assert config["auto.offset.reset"] == "earliest"


def test_add_consumer_requires_a_group_id(app):
    kafka = FlaskConfluentKafka(app)
    with pytest.raises(TypeError):
        kafka.add_consumer("orders")


def test_add_consumer_raises_for_a_duplicate_name(app):
    kafka = FlaskConfluentKafka(app)
    kafka.add_consumer("orders", group_id="orders-group")

    with pytest.raises(RuntimeError, match="already registered"):
        kafka.add_consumer("orders", group_id="a-different-group")


def test_add_consumer_raises_when_the_instance_has_no_initialized_app():
    kafka = FlaskConfluentKafka()
    with pytest.raises(RuntimeError, match="not initialized"):
        kafka.add_consumer("orders", group_id="orders-group")


def test_add_consumer_wraps_a_kafka_exception_in_a_runtime_error(app, mock_kafka_clients):
    _producer_cls, consumer_cls = mock_kafka_clients
    kafka = FlaskConfluentKafka(app)
    consumer_cls.side_effect = KafkaException("boom")

    with pytest.raises(RuntimeError, match="Failed to create Kafka consumer"):
        kafka.add_consumer("orders", group_id="orders-group")


def test_add_consumer_registers_an_atexit_hook_for_only_the_consumer(app, mock_atexit_register):
    kafka = FlaskConfluentKafka(app)
    mock_atexit_register.clear()

    consumer = kafka.add_consumer("orders", group_id="orders-group")

    assert len(mock_atexit_register) == 1
    _func, args, _kwargs = mock_atexit_register[0]
    assert args == (None, consumer)


def test_add_consumer_atexit_hook_closes_without_touching_a_producer(app, mock_atexit_register):
    kafka = FlaskConfluentKafka(app)
    mock_atexit_register.clear()
    consumer = kafka.add_consumer("orders", group_id="orders-group")
    func, args, kwargs = mock_atexit_register[0]

    func(*args, **kwargs)

    consumer.close.assert_called_once()


def test_add_consumer_registers_into_the_correct_apps_extensions_when_two_apps_share_one_extension(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    with app_a.app_context():
        consumer_a = kafka.add_consumer("orders", group_id="orders-group")
    with app_b.app_context():
        consumer_b = kafka.add_consumer("orders", group_id="orders-group")

    assert app_a.extensions["kafka_consumers"]["orders"] is consumer_a
    assert app_b.extensions["kafka_consumers"]["orders"] is consumer_b
    assert consumer_a is not consumer_b


def test_get_producer_returns_a_registered_producer(app):
    kafka = FlaskConfluentKafka(app)
    producer = kafka.add_producer("orders")
    assert kafka.get_producer("orders") is producer


def test_get_producer_raises_for_an_unregistered_name(app):
    kafka = FlaskConfluentKafka(app)
    with pytest.raises(RuntimeError, match="not registered"):
        kafka.get_producer("orders")


def test_get_consumer_returns_a_registered_consumer(app):
    kafka = FlaskConfluentKafka(app)
    consumer = kafka.add_consumer("orders", group_id="orders-group")
    assert kafka.get_consumer("orders") is consumer


def test_get_consumer_raises_for_an_unregistered_name(app):
    kafka = FlaskConfluentKafka(app)
    with pytest.raises(RuntimeError, match="not registered"):
        kafka.get_consumer("orders")


def test_get_producer_uses_the_correct_app_when_two_apps_share_one_extension(make_app):
    app_a, app_b = make_app("app-a"), make_app("app-b")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)
    kafka.init_app(app_b)

    with app_a.app_context():
        producer_a = kafka.add_producer("orders")
    with app_b.app_context():
        producer_b = kafka.add_producer("orders")

    with app_a.app_context():
        assert kafka.get_producer("orders") is producer_a
    with app_b.app_context():
        assert kafka.get_producer("orders") is producer_b
