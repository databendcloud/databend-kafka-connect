package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.ColumnDefinition;
import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.databendclient.DatabendConnection.StatementBinder;
import com.databend.kafka.connect.databendclient.TableDefinition;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SchemaPair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.isNull;

public class PreparedStatementBinder implements StatementBinder {

    private final DatabendSinkConfig.PrimaryKeyMode pkMode;
    private final PreparedStatement statement;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final DatabendSinkConfig.InsertMode insertMode;
    private final DatabendConnection dialect;
    private final TableDefinition tabDef;


    public PreparedStatementBinder(
            DatabendConnection dialect,
            PreparedStatement statement,
            DatabendSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tabDef,
            DatabendSinkConfig.InsertMode insertMode
    ) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.statement = statement;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
    }

    @Override
    public void bindRecord(SinkRecord record) throws SQLException {
        final Struct valueStruct = (Struct) record.value();
        final boolean isDelete = isNull(valueStruct);
        // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by
        //             nonKeyFieldNames, in iteration order for all INSERT/ UPSERT queries
        //             the relevant SQL has placeholders for keyFieldNames,
        //             in iteration order for all DELETE queries
        //             the relevant SQL has placeholders for nonKeyFieldNames first followed by
        //             keyFieldNames, in iteration order for all UPDATE queries

        int index = 1;
        if (isDelete) {
            bindKeyFields(record, index);
        } else {
            switch (insertMode) {
                case INSERT:
                case UPSERT:
                    index = bindKeyFields(record, index);
                    bindNonKeyFields(record, valueStruct, index);
                    break;

                case UPDATE:
                    index = bindNonKeyFields(record, valueStruct, index);
                    bindKeyFields(record, index);
                    break;
                default:
                    throw new AssertionError();

            }
        }
        statement.addBatch();
    }

    protected int bindKeyFields(SinkRecord record, int index) throws SQLException {
        switch (pkMode) {
            case NONE:
                if (!fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new AssertionError();
                }
                break;

            case KAFKA: {
                assert fieldsMetadata.keyFieldNames.size() == 3;
                bindField(index++, Schema.STRING_SCHEMA, record.topic(),
                        DatabendSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
                bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition(),
                        DatabendSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
                bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset(),
                        DatabendSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
            }
            break;

            case RECORD_KEY: {
                if (schemaPair.keySchema.type().isPrimitive()) {
                    assert fieldsMetadata.keyFieldNames.size() == 1;
                    bindField(index++, schemaPair.keySchema, record.key(),
                            fieldsMetadata.keyFieldNames.iterator().next());
                } else {
                    for (String fieldName : fieldsMetadata.keyFieldNames) {
                        final Field field = schemaPair.keySchema.field(fieldName);
                        bindField(index++, field.schema(), ((Struct) record.key()).get(field), fieldName);
                    }
                }
            }
            break;

            case RECORD_VALUE: {
                for (String fieldName : fieldsMetadata.keyFieldNames) {
                    final Field field = schemaPair.valueSchema.field(fieldName);
                    bindField(index++, field.schema(), ((Struct) record.value()).get(field), fieldName);
                }
            }
            break;

            default:
                throw new ConnectException("Unknown primary key mode: " + pkMode);
        }
        return index;
    }

    protected int bindNonKeyFields(
            SinkRecord record,
            Struct valueStruct,
            int index
    ) throws SQLException {
        for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
            final Field field = record.valueSchema().field(fieldName);
            bindField(index++, field.schema(), valueStruct.get(field), fieldName);
        }
        return index;
    }

    protected void bindField(int index, Schema schema, Object value, String fieldName)
            throws SQLException {
        ColumnDefinition colDef = tabDef == null ? null : tabDef.definitionForColumn(fieldName);
        dialect.bindField(statement, index, schema, value, colDef);
    }
}

