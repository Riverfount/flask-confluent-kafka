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

See the [Configuration](configuration.md) reference for every available `app.config` key, and the [API Reference](api.md) for the full method signatures.
