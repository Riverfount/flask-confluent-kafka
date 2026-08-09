# Configuration

All configuration is read from `app.config` in `init_app`:

| Config key        | Default          | Maps to               | Description                            |
|--------------------|------------------|------------------------|------------------------------------------|
| `KAFKA_SERVER`     | `localhost:9092` | `bootstrap.servers`   | Comma-separated list of Kafka brokers   |
| `KAFKA_USERNAME`   | `""`             | `sasl.username` *     | SASL username                           |
| `KAFKA_PASSWORD`   | `""`             | `sasl.password` *     | SASL password                           |
| `KAFKA_PROTOCOL`   | `PLAINTEXT`      | `security.protocol`   | e.g. `PLAINTEXT`, `SASL_SSL`            |
| `KAFKA_MECHANISM`  | `PLAIN`          | `sasl.mechanism` *    | e.g. `PLAIN`, `SCRAM-SHA-256`           |
| `KAFKA_GROUP_ID`   | `default_group`  | `group.id` (consumer) | Consumer group id                       |

\* `sasl.username`, `sasl.password`, and `sasl.mechanism` are only added to the client config when `KAFKA_PROTOCOL` is `SASL_PLAINTEXT` or `SASL_SSL` (matched case-insensitively). For `PLAINTEXT` (the default) or plain `SSL`, these three keys are left out of the config passed to `confluent_kafka.Producer`/`Consumer` entirely.

Example with SASL authentication:

```python
app.config["KAFKA_SERVER"] = "broker:9092"
app.config["KAFKA_USERNAME"] = "my-user"
app.config["KAFKA_PASSWORD"] = "my-password"
app.config["KAFKA_PROTOCOL"] = "SASL_SSL"
app.config["KAFKA_MECHANISM"] = "PLAIN"
```
