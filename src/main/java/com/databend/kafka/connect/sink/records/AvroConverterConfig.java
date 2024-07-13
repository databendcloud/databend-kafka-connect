package com.databend.kafka.connect.sink.records;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Map;

class AvroConverterConfig extends AbstractKafkaAvroSerDeConfig {
    AvroConverterConfig(final Map<?, ?> props) {
        super(baseConfigDef(), props);
    }
}
