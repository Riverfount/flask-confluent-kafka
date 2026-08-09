from unittest.mock import MagicMock

import pytest

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
    producer.flush.assert_called_once()


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


def test_produce_raises_for_an_active_but_uninitialized_app(make_app):
    app_a, app_c = make_app("app-a"), make_app("app-c")
    kafka = FlaskConfluentKafka()
    kafka.init_app(app_a)  # only app_a is initialized

    with app_c.app_context(), pytest.raises(RuntimeError, match="producer is not initialized"):
        kafka.produce("topic", "value")
