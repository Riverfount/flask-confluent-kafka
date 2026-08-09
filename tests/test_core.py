from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaException

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
