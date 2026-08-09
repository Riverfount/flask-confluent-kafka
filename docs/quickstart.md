# Quickstart

```python
from flask import Flask
from flask_confluent_kafka import FlaskConfluentKafka

app = Flask(__name__)
app.config["KAFKA_SERVER"] = "localhost:9092"
app.config["KAFKA_GROUP_ID"] = "my-app-group"

kafka = FlaskConfluentKafka(app)

# Produce a message (dicts are JSON-encoded automatically)
kafka.produce("my-topic", {"hello": "world"})

# Consume a single message (returns None if nothing arrives within `timeout`)
message = kafka.consume(["my-topic"])
if message is not None:
    print(message)
```

## Application factory pattern

```python
kafka = FlaskConfluentKafka()

def create_app():
    app = Flask(__name__)
    kafka.init_app(app)
    return app
```

A single `FlaskConfluentKafka()` instance can be `init_app()`'d against more than one app. Calls to `produce()`/`consume()` made while a given app's context is active always use that app's own producer/consumer — see [API Reference](api.md) for how client resolution works.

## Multiple producers/consumers

Register additional named producers/consumers, independent of the default pair:

```python
orders_producer = kafka.add_producer("orders")
orders_consumer = kafka.add_consumer("orders", group_id="orders-processor")

orders_producer.produce("orders", value=b'{"id": 1}')
orders_producer.poll(0)

orders_consumer.subscribe(["orders"])
msg = orders_consumer.poll(1.0)
```

Fetch one later, from anywhere else in the app, with `kafka.get_producer("orders")` / `kafka.get_consumer("orders")`. See [API Reference](api.md) for the full `add_producer()`/`add_consumer()`/`get_producer()`/`get_consumer()` reference.

See the [Configuration](configuration.md) reference for every available `app.config` key, and the [API Reference](api.md) for the full method signatures.
