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
    - After building, you will get the jar files in `target` dir. Or download the package jar files from [release](https://github.com/databendcloud/databend-kafka-connect/releases) Just put it into kafka libs dir.
    - Create `databend.properties` file in kafka config dir and config it: `nano config/databend.properties`, you can check the example
      configuration
      in [application.properties.example](src/main/resources/conf/application.properties.example)
    - config your source connect such as kafka mysql connector
    - if you sync data from kafka topic directly, please confirm the data format from kafka topic, for example, if your data if AVRO format, you should add the following config into config properties:
 
  ```
    key.converter=org.apache.kafka.connect.storage.StringConverter
    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.basic.auth.credentials.source=USER_INFO
    value.converter.schema.registry.basic.auth.user.info=xxxx
    value.converter.schema.registry.url=https://your-registry-url.us-east-2.aws.confluent.cloud
  ```
  - Start or restart the Kafka Connect workers.: `bin/connect-standalone.sh config/connect-standalone.properties config/databend.properties config/mysql.properties`

More details about configure kafka connector, please read [Configure Self-Managed Connectors](https://docs.confluent.io/platform/current/connect/configuring.html).

# Contributing

You are warmly welcome to hack on debezium-server-databend. We have prepared a guide [CONTRIBUTING.md](./CONTRIBUTING.md).

