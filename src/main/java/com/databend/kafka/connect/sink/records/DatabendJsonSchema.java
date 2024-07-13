package com.databend.kafka.connect.sink.records;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/** databend json schema */
public class DatabendJsonSchema implements Schema {
    static String NAME = "DATABEND_JSON_SCHEMA";
    static int VERSION = 1;

    @Override
    public Type type() {
        return Type.STRUCT;
    }

    @Override
    public boolean isOptional() {
        return false;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Integer version() {
        return VERSION;
    }

    @Override
    public String doc() {
        return null;
    }

    @Override
    public Map<String, String> parameters() {
        return null;
    }

    @Override
    public Schema keySchema() {
        // Create and return a Schema representing a string type
        return null;
    }

    @Override
    public Schema valueSchema() {
        // Create and return a Schema representing a string type
        return null;
    }

    @Override
    public List<Field> fields() {
        return null;
    }

    @Override
    public Field field(final String s) {
        return null;
    }

    @Override
    public Schema schema() {
        return null;
    }
}

