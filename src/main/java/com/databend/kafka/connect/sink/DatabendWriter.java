package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.CachedConnectionProvider;
import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.databendclient.TableIdentity;
import com.databend.kafka.connect.sink.records.Data;
import com.databend.kafka.connect.sink.records.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DatabendWriter {
    private static final Logger log = LoggerFactory.getLogger(DatabendWriter.class);

    private final DatabendSinkConfig config;
    private final DatabendConnection dbConnection;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;

    DatabendWriter(final DatabendSinkConfig config, DatabendConnection dbConnection, DbStructure dbStructure) {
        this.config = config;
        this.dbConnection = dbConnection;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = connectionProvider(
                config.connectionAttempts,
                config.connectionBackoffMs
        );
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dbConnection, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("DatabendWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    public void writeSchemaLessData(final Collection<Record> records) throws SQLException, TableAlterOrCreateException {
        final com.databend.jdbc.DatabendConnection connection = (com.databend.jdbc.DatabendConnection) cachedConnectionProvider.getConnection();
        log.info("DatabendWriter Writing {} records", records.size());
        ObjectMapper objectMapper = new ObjectMapper();

        StringBuilder sb = new StringBuilder();
        try {
            TableIdentity tableId = null;
            for (Record record : records) {
                tableId = destinationTable(record.getTopic());
                Map<String, Data> recordMap = record.getJsonMap();

                // create a new map to store the transformed data
                Map<String, Object> transformedMap = new HashMap<>();

                for (Map.Entry<String, Data> entry : recordMap.entrySet()) {
                    String key = entry.getKey();
                    Data data = entry.getValue();

                    // Check the field type and handle the object accordingly
                    Object value;
                    switch (data.getFieldType()) {
                        case INT8:
                        case INT16:
                        case INT32:
                        case INT64:
                            value = Integer.parseInt(data.getObject().toString());
                            break;
                        case FLOAT32:
                        case FLOAT64:
                            value = Double.parseDouble(data.getObject().toString());
                            break;
                        case BOOLEAN:
                            value = Boolean.parseBoolean(data.getObject().toString());
                            break;
                        case STRING:
                            value = data.getObject().toString();
                            break;
                        default:
                            value = data.getObject().toString();
                            break;
                    }

                    // Add the processed value to the map
                    transformedMap.put(key, value);
                }
                log.info("DatabendWriter Writing transformedMap is: {}", transformedMap);

                String json = objectMapper.writeValueAsString(transformedMap);
                sb.append(json).append("\n");
            }

            // if there are no records to write, return
            if (sb.length() == 0) {
                log.info("No records to write");
                return;
            }

            String jsonStr = sb.toString();

            // create uuid for the stage path
            String uuid = UUID.randomUUID().toString();
            String stagePath = String.format("%s/%s/%s/%s/%s/%s/%s",
                    LocalDateTime.now().getYear(),
                    LocalDateTime.now().getMonthValue(),
                    LocalDateTime.now().getDayOfMonth(),
                    LocalDateTime.now().getHour(),
                    LocalDateTime.now().getMinute(),
                    LocalDateTime.now().getSecond(),
                    uuid);

            String fileName = String.format("%s.ndjson", uuid);

            byte[] jsonBytes = jsonStr.getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(jsonBytes);
            int contentLength = jsonBytes.length;

            log.info("Uploading data, file size: {} bytes", contentLength);

            connection.uploadStream("~", stagePath, inputStream, fileName, contentLength, false);

            assert tableId != null;

            String stagePlusFileName = String.format("@~/%s/%s", stagePath, fileName);

            log.info("Copying data from stage: {}", stagePlusFileName);

            String copyIntoSQL = String.format(
                    "COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO) " +
                            "PURGE = %b FORCE = %b DISABLE_VARIANT_CHECK = %b",
                    tableId,
                    stagePlusFileName,
                    true,
                    true,
                    true
            );

            try {
                log.info("Executing COPY INTO SQL: {}", copyIntoSQL);
                connection.createStatement().execute(copyIntoSQL);
                log.info("COPY INTO completed successfully");
            } catch (Exception e) {
                log.error("DatabendWriter writeSchemaLessData error: {}", e);
                throw e; // throw the exception to the caller
            }
        } catch (TableAlterOrCreateException e) {
            throw e;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    void write(final Collection<SinkRecord> records)
            throws SQLException, TableAlterOrCreateException {
        final Connection connection = cachedConnectionProvider.getConnection();
        log.info("DatabendWriter Writing {} records", records.size());
        try {
            final Map<TableIdentity, BufferedRecords> bufferByTable = new HashMap<>();
            for (SinkRecord record : records) {
                log.info("DatabendWriter Writing record keySchema is: {}", record.keySchema());
                if (record.valueSchema() != null) {
                    log.info("DatabendWriter Writing record valueSchema is: {}", record.valueSchema().fields());
                }
                log.info("DatabendWriter Writing record key is: {}", record.key());
                log.info("DatabendWriter Writing record topic is: {}", record.topic());
                log.info("DatabendWriter Writing record timestamp is: {}", record.timestamp());
                final TableIdentity tableId = destinationTable(record.topic());
                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbConnection, dbStructure, connection);
                    bufferByTable.put(tableId, buffer);
                }
                buffer.add(record);
            }
            for (Map.Entry<TableIdentity, BufferedRecords> entry : bufferByTable.entrySet()) {
                TableIdentity tableId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in Databend Writer for table ID: {}", tableId);
                buffer.flush();
                buffer.close();
            }
//            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
//            e.addSuppressed(e);
            throw e;
        }
    }

    void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableIdentity destinationTable(String topic) {
        final String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format(
                    "Destination table name for topic '%s' is empty using the format string '%s'",
                    topic,
                    config.tableNameFormat
            ));
        }
        return dbConnection.parseTableIdentifier(tableName);
    }
}

