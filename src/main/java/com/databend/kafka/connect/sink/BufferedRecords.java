package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.ColumnIdentity;
import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.databendclient.DatabendConnection.StatementBinder;
import com.databend.kafka.connect.databendclient.TableIdentity;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SchemaPair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableIdentity tableId;
    private final DatabendSinkConfig config;
    private final DatabendConnection dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private RecordValidator recordValidator;
    private FieldsMetadata fieldsMetadata;
    private PreparedStatement updatePreparedStatement;
    private PreparedStatement deletePreparedStatement;
    private StatementBinder updateStatementBinder;
    private StatementBinder deleteStatementBinder;
    private boolean deletesInBatch = false;

    public BufferedRecords(
            DatabendSinkConfig config,
            TableIdentity tableId,
            DatabendConnection dbDialect,
            DbStructure dbStructure,
            Connection connection
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
        this.recordValidator = RecordValidator.create(config);
    }

    public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
        recordValidator.validate(record);
        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (isNull(record.valueSchema())) {
            // For deletes, value and optionally value schema come in as null.
            // We don't want to treat this as a schema change if key schemas is the same
            // otherwise we flush unnecessarily.
            if (config.deleteEnabled) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.valueSchema())) {
            if (config.deleteEnabled && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        } else {
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }
        if (schemaChanged || updateStatementBinder == null) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());

            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );
            fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    config.pkMode,
                    config.pkFields,
                    config.fieldsWhitelist,
                    schemaPair
            );
            dbStructure.createOrAmendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata
            );
            final String insertSql = getInsertSql();
            final String deleteSql = getDeleteSql();
            log.debug(
                    "{} sql: {} deleteSql: {} meta: {}",
                    config.insertMode,
                    insertSql,
                    deleteSql,
                    fieldsMetadata
            );
            close();
            updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
            updateStatementBinder = dbDialect.statementBinder(
                    updatePreparedStatement,
                    config.pkMode,
                    schemaPair,
                    fieldsMetadata,
                    dbStructure.tableDefinition(connection, tableId),
                    config.insertMode
            );
            if (config.deleteEnabled && nonNull(deleteSql)) {
                deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
                deleteStatementBinder = dbDialect.statementBinder(
                        deletePreparedStatement,
                        config.pkMode,
                        schemaPair,
                        fieldsMetadata,
                        dbStructure.tableDefinition(connection, tableId),
                        config.insertMode
                );
            }
        }

        // set deletesInBatch if schema value is not null
        if (isNull(record.value()) && config.deleteEnabled) {
            deletesInBatch = true;
        }

        records.add(record);

        if (records.size() >= config.batchSize) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord record : records) {
            if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
                deleteStatementBinder.bindRecord(record);
            } else {
                updateStatementBinder.bindRecord(record);
            }
        }
        executeUpdates();
        executeDeletes();

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        deletesInBatch = false;
        return flushedRecords;
    }

    private void executeUpdates() throws SQLException {
        int[] batchStatus = updatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException(
                        "Execution failed for part of the batch update", batchStatus);
            }
        }
    }

    private void executeDeletes() throws SQLException {
        if (nonNull(deletePreparedStatement)) {
            int[] batchStatus = deletePreparedStatement.executeBatch();
            for (int updateCount : batchStatus) {
                if (updateCount == Statement.EXECUTE_FAILED) {
                    throw new BatchUpdateException(
                            "Execution failed for part of the batch delete", batchStatus);
                }
            }
        }
    }

    public void close() throws SQLException {
        log.debug(
                "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
                updatePreparedStatement,
                deletePreparedStatement
        );
        if (nonNull(updatePreparedStatement)) {
            updatePreparedStatement.close();
            updatePreparedStatement = null;
        }
        if (nonNull(deletePreparedStatement)) {
            deletePreparedStatement.close();
            deletePreparedStatement = null;
        }
    }

    private String getInsertSql() throws SQLException {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames),
                            dbStructure.tableDefinition(connection, tableId)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported.",
                            tableId
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete.");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.keyFieldNames)
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported.",
                                tableId
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnIdentity> asColumns(Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnIdentity(tableId, name))
                .collect(Collectors.toList());
    }
}

