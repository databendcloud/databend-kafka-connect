package com.databend.kafka.connect.sink.records;

import org.apache.kafka.connect.data.Schema;

public class Data {
    private Schema.Type fieldType;
    private Object object;

    public Data(Schema.Type fieldType, Object object) {
        this.fieldType = fieldType;
        this.object = object;
    }

    public Schema.Type getFieldType() {
        return fieldType;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public String toString() {
        if (object == null) {
            return null;
        }
        return object.toString();
    }
    public String getJsonType() {
        switch (fieldType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return "integer";
            case FLOAT32:
            case FLOAT64:
                return "number";
            case BOOLEAN:
                return "boolean";
            case STRING:
                return "string";
            default:
                return "string";
        }
    }
}

