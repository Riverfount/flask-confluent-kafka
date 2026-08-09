# API Reference

## `FlaskConfluentKafka(app=None)`

Creates the extension. If `app` is given, calls `init_app(app)` immediately; otherwise, call `init_app(app)` yourself later (application factory pattern).

## `init_app(app)`

Reads the config keys described in [Configuration](configuration.md), creates a `confluent_kafka.Producer` and `confluent_kafka.Consumer` — the **default pair** — and stores them in `app.extensions["kafka_producer"]` / `app.extensions["kafka_consumer"]`. For additional producers/consumers, see `add_producer()`/`add_consumer()` below.

## Client resolution (multiple apps)

`produce()`/`consume()` don't just reuse whichever app was `init_app()`'d last. They resolve the producer/consumer in this order:

1. If a Flask application context is active (`flask.has_app_context()`), use `current_app.extensions`.
2. Otherwise, fall back to the app passed to the constructor (or the last one passed to `init_app()`, for the no-arg constructor case).
3. If neither yields a client, raise `RuntimeError`.

This means a single `FlaskConfluentKafka()` instance shared across multiple apps is safe as long as calls happen inside the relevant app's context (e.g. during a request, or inside `with app.app_context():`). The one edge case not covered: calling `produce()`/`consume()` with **no context active at all** when the instance has been `init_app()`'d against more than one app — the fallback then uses whichever app was initialized last.

`add_producer()`, `add_consumer()`, `get_producer()`, and `get_consumer()` resolve the active app the same way, with one additional requirement: the resolved app must already have been `init_app()`'d (`add_producer()`/`add_consumer()` register *additional* clients for an app already using this extension — they don't stand in for `init_app()`). Calling any of the four for an app that hasn't been `init_app()`'d — including an unrelated app that merely happens to have its context active — raises `RuntimeError`.

## `produce(topic: str, value: dict | str, key: str | None = None) -> None`

Queues a message for asynchronous delivery to `topic` (`dict` values are JSON-serialized; `str` values are UTF-8 encoded) and polls once, non-blocking, to serve any already-completed delivery callbacks. This doesn't wait for broker acknowledgment — call `flush()` on `app.extensions["kafka_producer"]` directly for a synchronous delivery guarantee. Raises `RuntimeError` if the producer isn't initialized, or if `produce()` itself fails (e.g. local queue full); broker-side delivery failures aren't surfaced here.

## `consume(topics: list[str], timeout: float = 1.0) -> str | None`

Polls for a single message on `topics`, returning its decoded value, or `None` if nothing arrived within `timeout` seconds — or if a non-fatal consumer error occurred (e.g. `KafkaError._PARTITION_EOF`, or any other error librdkafka doesn't flag as fatal; logged as a warning). Raises `RuntimeError` only for a fatal consumer error (`KafkaError.fatal()` is `True`), since that signals the client itself is broken and can't recover. Subscribes the consumer to `topics` the first time it's called (or whenever the requested topic set changes), not on every call — so a `while True: consume(...)` loop doesn't trigger a consumer-group rebalance on each poll.

## `add_producer(name: str, config_overrides: dict[str, Any] | None = None) -> Producer`

Creates and registers an additional named `confluent_kafka.Producer`, independent of the default one created by `init_app()`. Uses the same connection config `init_app()` builds from `app.config` (`bootstrap.servers`, `security.protocol`, and `sasl.*` when applicable), with `config_overrides` layered on top — so `config_overrides` can override anything, including `bootstrap.servers` itself, e.g. to point this producer at a different cluster. Stores the result in `app.extensions["kafka_producers"][name]` and registers its own `atexit` shutdown hook that flushes it on process exit. Raises `RuntimeError` if this instance hasn't been `init_app()`'d for the active app yet, if `name` is already registered, or if `Producer` construction itself fails.

## `add_consumer(name: str, *, group_id: str, config_overrides: dict[str, Any] | None = None) -> Consumer`

Creates and registers an additional named `confluent_kafka.Consumer`, independent of the default one created by `init_app()`. `group_id` is required — unlike the default consumer, it never falls back to `KAFKA_GROUP_ID`, since silently sharing a group id with another consumer would just make it join that consumer group and compete for partitions with it instead of running independently. Uses the same connection config as `add_producer()`, plus `group.id=group_id` and `auto.offset.reset="earliest"`, with `config_overrides` layered on top of all of that. Stores the result in `app.extensions["kafka_consumers"][name]` and registers its own `atexit` shutdown hook that closes it on process exit. Raises `RuntimeError` under the same conditions as `add_producer()`.

## `get_producer(name: str) -> Producer` / `get_consumer(name: str) -> Consumer`

Look up a producer/consumer previously registered with `add_producer()`/`add_consumer()` for the active app. Raises `RuntimeError` if none is registered under `name`.

## Shutdown

`init_app()` registers an [`atexit`](https://docs.python.org/3/library/atexit.html) hook per app that flushes its producer (bounded by a 10 second timeout) and closes its consumer when the Python process exits. This is intentionally not wired to Flask's `app.teardown_appcontext()` — that fires after every request, not at process shutdown, which would tear down the producer/consumer after the very first request instead of once at the end.

`add_producer()`/`add_consumer()` each register their own `atexit` hook the same way, scoped to just the one client they created.
