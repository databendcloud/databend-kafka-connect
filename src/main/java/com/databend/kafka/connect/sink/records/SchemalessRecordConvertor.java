package com.databend.kafka.connect.sink.records;

import com.databend.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemalessRecordConvertor implements RecordConvertor {

    @Override
    public Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        List<Field> fields = new ArrayList<>();
        Map<?,?> map = (Map) sinkRecord.value();
        Map<String, Data> data = new HashMap<>();
        int index = 0;
        map.forEach((key,val) -> {
            Schema.Type type;
            Object value;

            if (val instanceof String) {
                type = Schema.Type.STRING;
                value = val.toString();
            } else if (val instanceof Integer) {
                type = Schema.Type.INT32;
                value = Integer.parseInt(val.toString());
            } else if (val instanceof Long) {
                type = Schema.Type.INT64;
                value = Long.parseLong(val.toString());
            } else if (val instanceof Double) {
                type = Schema.Type.FLOAT64;
                value = Double.parseDouble(val.toString());
            } else if (val instanceof Boolean) {
                type = Schema.Type.BOOLEAN;
                value = Boolean.parseBoolean(val.toString());
            } else {
                type = Schema.Type.STRING;
                value = val == null ? null : val.toString();
            }

            fields.add(new Field(key.toString(), index, Schema.STRING_SCHEMA));
            data.put(key.toString(), new Data(type, value));
        });
        return new Record(SchemaType.SCHEMA_LESS, new OffsetContainer(topic, partition, offset), fields, data, sinkRecord);
    }
}

