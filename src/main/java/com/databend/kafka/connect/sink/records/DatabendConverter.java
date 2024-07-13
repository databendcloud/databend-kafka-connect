package com.databend.kafka.connect.sink.records;

import com.databend.jdbc.com.fasterxml.jackson.databind.ObjectMapper;
import com.databend.kafka.connect.sink.DatabendSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Databend Converter
 */
public abstract class DatabendConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(DatabendSinkConfig.class);
    final ObjectMapper mapper = new ObjectMapper();

    /**
     * unused
     */
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
        // not necessary
    }

    /**
     * doesn't support data source connector
     */
    @Override
    public byte[] fromConnectData(final String s, final Schema schema, final Object o) {
        throw new UnsupportedOperationException("DatabendConverter doesn't support data source connector");
    }
}

