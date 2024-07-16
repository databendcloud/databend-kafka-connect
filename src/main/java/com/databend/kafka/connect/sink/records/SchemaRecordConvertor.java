package com.databend.kafka.connect.sink.records;

import com.databend.kafka.connect.sink.kafka.OffsetContainer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public class SchemaRecordConvertor implements RecordConvertor{

    @Override
    public Record convert(SinkRecord sinkRecord) {
        String topic = sinkRecord.topic();
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        Struct struct = (Struct) sinkRecord.value();
        Map<String, Data> data = StructToJsonMap.toJsonMap((Struct) sinkRecord.value());
        return new Record(SchemaType.SCHEMA, new OffsetContainer(topic, partition, offset), struct.schema().fields(), data, sinkRecord);
    }
}
