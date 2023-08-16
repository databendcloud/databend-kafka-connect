package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.util.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import static com.databend.kafka.connect.sink.DatabendSinkConfig.PrimaryKeyMode.*;

@FunctionalInterface
public interface RecordValidator {

    RecordValidator NO_OP = (record) -> { };

    void validate(SinkRecord record);

    default RecordValidator and(RecordValidator other) {
        if (other == null || other == NO_OP || other == this) {
            return this;
        }
        if (this == NO_OP) {
            return other;
        }
        RecordValidator thisValidator = this;
        return (record) -> {
            thisValidator.validate(record);
            other.validate(record);
        };
    }

    static RecordValidator create(DatabendSinkConfig config) {
        RecordValidator requiresKey = requiresKey(config);
        RecordValidator requiresValue = requiresValue(config);

        RecordValidator keyValidator = NO_OP;
        RecordValidator valueValidator = NO_OP;
        switch (config.pkMode) {
            case RECORD_KEY:
                keyValidator = keyValidator.and(requiresKey);
                break;
            case RECORD_VALUE:
            case NONE:
                valueValidator = valueValidator.and(requiresValue);
                break;
            case KAFKA:
            default:
                // no primary key is required
                break;
        }

        if (config.deleteEnabled) {
            // When delete is enabled, we need a key
            keyValidator = keyValidator.and(requiresKey);
        } else {
            // When delete is disabled, we need non-tombstone values
            valueValidator = valueValidator.and(requiresValue);
        }

        // Compose the validator that may or may be NO_OP
        return keyValidator.and(valueValidator);
    }

    static RecordValidator requiresValue(DatabendSinkConfig config) {
        return record -> {
            Schema valueSchema = record.valueSchema();
            if (record.value() != null
                    && valueSchema != null
                    && valueSchema.type() == Schema.Type.STRUCT) {
                return;
            }
            throw new ConnectException(
                    String.format(
                            "Databend Sink connector is configured with '%s=%s' and '%s=%s' and therefore requires "
                                    + "records with a non-null Struct value and non-null Struct schema, "
                                    + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
                                    + "with a %s value and %s value schema.",
                            DatabendSinkConfig.DELETE_ENABLED,
                            config.deleteEnabled,
                            DatabendSinkConfig.PK_MODE,
                            config.pkMode.toString().toLowerCase(),
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset(),
                            record.timestamp(),
                            StringUtils.valueTypeOrNull(record.value()),
                            StringUtils.schemaTypeOrNull(record.valueSchema())
                    )
            );
        };
    }

    static RecordValidator requiresKey(DatabendSinkConfig config) {
        return record -> {
            Schema keySchema = record.keySchema();
            if (record.key() != null
                    && keySchema != null
                    && (keySchema.type() == Schema.Type.STRUCT || keySchema.type().isPrimitive())) {
                return;
            }
            throw new ConnectException(
                    String.format(
                            "Databend Sink connector is configured with '%s=%s' and '%s=%s' and therefore requires "
                                    + "records with a non-null key and non-null Struct or primitive key schema, "
                                    + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
                                    + "with a %s key and %s key schema.",
                            DatabendSinkConfig.DELETE_ENABLED,
                            config.deleteEnabled,
                            DatabendSinkConfig.PK_MODE,
                            config.pkMode.toString().toLowerCase(),
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset(),
                            record.timestamp(),
                            StringUtils.valueTypeOrNull(record.key()),
                            StringUtils.schemaTypeOrNull(record.keySchema())
                    )
            );
        };
    }
}

