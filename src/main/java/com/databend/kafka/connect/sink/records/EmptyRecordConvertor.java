package com.databend.kafka.connect.sink.records;

import com.databend.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class EmptyRecordConvertor implements RecordConvertor {
    @Override
    public Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        List<Field> fields = new ArrayList<>();
        return new Record(SchemaType.SCHEMA_LESS, new OffsetContainer(topic, partition, offset), fields, null, sinkRecord);
    }
}