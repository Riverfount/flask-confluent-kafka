# Configuration

All configuration is read from `app.config` in `init_app`:

| Config key        | Default          | Maps to               | Description                            |
|--------------------|------------------|------------------------|------------------------------------------|
| `KAFKA_SERVER`     | `localhost:9092` | `bootstrap.servers`   | Comma-separated list of Kafka brokers   |
| `KAFKA_USERNAME`   | `""`             | `sasl.username`       | SASL username                           |
| `KAFKA_PASSWORD`   | `""`             | `sasl.password`       | SASL password                           |
| `KAFKA_PROTOCOL`   | `PLAINTEXT`      | `security.protocol`   | e.g. `PLAINTEXT`, `SASL_SSL`            |
| `KAFKA_MECHANISM`  | `PLAIN`          | `sasl.mechanism`      | e.g. `PLAIN`, `SCRAM-SHA-256`           |
| `KAFKA_GROUP_ID`   | `default_group`  | `group.id` (consumer) | Consumer group id                       |

Example with SASL authentication:

```python
app.config["KAFKA_SERVER"] = "broker:9092"
app.config["KAFKA_USERNAME"] = "my-user"
app.config["KAFKA_PASSWORD"] = "my-password"
app.config["KAFKA_PROTOCOL"] = "SASL_SSL"
app.config["KAFKA_MECHANISM"] = "PLAIN"
```
