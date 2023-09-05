# Databend Kafka Connect Sink
**The connector is available in beta stage for early adopters. If you notice a problem, please [file an issue.](https://github.com/databendcloud/databend-kafka-connect/issues/new)**

## About
databend-kafka-connect is the official Kafka Connect sink connector for [Databend](https://databend.rs/).
The Kafka connector delivers data from a Kafka topic to a Databend table.
The detail introduction docs available in [docs page](./docs/docs.md)


## Install from source

- Requirements:
    - JDK 11
    - Maven
- Clone from repo: `git clone https://github.com/databendcloud/databend-kafka-connect.git`
- From the root of the project:
    - Build and package debezium server: `mvn -Passembly -Dmaven.test.skip package`
    - After building, you will get the jar file in `target` dir. Just put it into kafka libs dir.
    - Create `databend.properties` file in kafka config dir and config it: `nano config/databend.properties`, you can check the example
      configuration
      in [application.properties.example](src/main/resources/conf/application.properties.example)
    - config your source connect such as kafka mysql connector
    - Start or restart the Kafka Connect workers.: `bin/connect-standalone.sh config/connect-standalone.properties config/databend.properties config/mysql.properties`

# Contributing

You are warmly welcome to hack on debezium-server-databend. We have prepared a guide [CONTRIBUTING.md](./CONTRIBUTING.md).

