class FlaskConfluentKafkaError(RuntimeError):
    """Base class for all errors raised by this extension.

    Every subclass also extends RuntimeError, so existing
    `except RuntimeError` call sites keep working unchanged.
    """


class NotInitializedError(FlaskConfluentKafkaError):
    """Raised when produce()/consume()/add_producer()/add_consumer() are
    called before init_app() for the active app.
    """


class AlreadyRegisteredError(FlaskConfluentKafkaError):
    """Raised by add_producer()/add_consumer() for a duplicate name."""


class NotRegisteredError(FlaskConfluentKafkaError):
    """Raised by get_producer()/get_consumer() for an unknown name."""


class ClientCreationError(FlaskConfluentKafkaError):
    """Raised when constructing a Producer/Consumer fails, wrapping the
    underlying KafkaException.
    """


class ProduceError(FlaskConfluentKafkaError):
    """Raised when produce() itself fails (e.g. local queue full)."""


class ConsumeError(FlaskConfluentKafkaError):
    """Raised when consume() encounters a fatal consumer error."""
