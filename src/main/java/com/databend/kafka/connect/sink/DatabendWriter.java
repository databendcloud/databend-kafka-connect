package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.CachedConnectionProvider;
import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.databendclient.TableIdentity;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

    void write(final Collection<SinkRecord> records)
            throws SQLException, TableAlterOrCreateException {
        final Connection connection = cachedConnectionProvider.getConnection();
        try {
            final Map<TableIdentity, BufferedRecords> bufferByTable = new HashMap<>();
            for (SinkRecord record : records) {
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
            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                e.addSuppressed(sqle);
            }
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

