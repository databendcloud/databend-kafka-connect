# Databend kafka connect

Databend kafka connect consume kafka events and write to destination databend tables. It is possible to
replicate source database one
to one or run it with `insert` mode or `upsert` mode to keep all change events in databend table. When event and key
schema
enabled (`debezium.format.value.schemas.enable=true`, `debezium.format.key.schemas.enable=true`) destination databend
tables created automatically with initial job.

#### Configuration properties

| Config                  | Default                                            | Description                                                                                                                           |
|-------------------------|----------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `name`                  | ``                                                 | Custom connector name                                                                                                                 |
| `connector.class`       | `com.databend.kafka.connect.DatabendSinkConnector` | Connector main class                                                                                                                  |
| `connection.url`        | ``                                                 | Databend database jdbc url, example: jdbc:databend://localhost:8000.                                                                  |
| `connection.ssl`        | `false`                                            | Connection ssl true or false.                                                                                                         |
| `connection.username`   | ``                                                 | Databend database user name.                                                                                                          |
| `connection.password`   | ``                                                 | Databend database user password.                                                                                                      |
| `connection.database`   | `default`                                          | Target Database name.                                                                                                                 |
| `connection.attempts`   | `3`                                                | Attempts to retry connection.                                                                                                         |
| `connection.backoff.ms` | `10000L`                                           | Backoff time in milliseconds between connection attempts.                                                                             |
| `table.name.format`     | `${topic}`                                         | A format string for the destination table name, which may contain '${topic}' as a placeholder for the originating topic name.         |
| `batch.size`            | `10`                                               | Specifies how many records to attempt to batch together for insertion into the destination tables.                                    |
| `auto.create`           | `false`                                            | Whether to automatically create the destination table based on record schema if it is found to be missing by isusing ``CREATE``.      |
| `auto.evolve`           | `false`                                            | Whether to automatically add columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``. |
| `insert.mode`           | `insert`                                           | The insertion mode to use. Supported modes are: insert, upsert.                                                                       |
| `pk.fields`             | ``                                                 | List of comma-separated primary key field names.                                                                                      |
| `topics`                | ``                                                 | List of comma-separated topics.                                                                                                       |
| `topic.prefix`          | ``                                                 | Listen topics prefix.                                                                                                                 |

If you want much more kafka connect parameters, see [Appendix F. Kafka Connect configuration parameters](https://access.redhat.com/documentation/zh-tw/red_hat_amq/7.4/html/using_amq_streams_on_red_hat_enterprise_linux_rhel/kafka-connect-configuration-parameters-str).