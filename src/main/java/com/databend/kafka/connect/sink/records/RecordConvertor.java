package com.databend.kafka.connect.sink.records;


import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordConvertor {
    Record convert(SinkRecord sinkRecord);
}