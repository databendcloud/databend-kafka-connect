package com.databend.kafka.connect.databendclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple cache of {@link TableDefinition} keyed.
 */
public class TableDefinitions {

    private static final Logger log = LoggerFactory.getLogger(TableDefinitions.class);

    private final Map<TableIdentity, TableDefinition> cache = new HashMap<>();
    private final DatabendConnection dialect;

    /**
     * Create an instance that uses the specified database dialect.
     *
     * @param dialect the database dialect; may not be null
     */
    public TableDefinitions(DatabendConnection dialect) {
        this.dialect = dialect;
    }

    /**
     * Get the {@link TableDefinition} for the given table.
     *
     * @param connection the Databend connection to use; may not be null
     * @param tableId    the table identifier; may not be null
     * @return the cached {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition get(
            Connection connection,
            final TableIdentity tableId
    ) throws SQLException {
        TableDefinition dbTable = cache.get(tableId);
        if (dbTable == null) {
            log.info("Table {} not found in cache; checking database", tableId);
            if (dialect.tableExists(connection, tableId)) {
                log.info("Table {} exists in database", tableId);
                dbTable = dialect.describeTable(connection, tableId);
                if (dbTable != null) {
                    log.info("Setting metadata for table {} to {}", tableId, dbTable);
                    cache.put(tableId, dbTable);
                }
            }
        }
        return dbTable;
    }

    /**
     * Refresh the cached {@link TableDefinition} for the given table.
     *
     * @param connection the Databend connection to use; may not be null
     * @param tableId    the table identifier; may not be null
     * @return the refreshed {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition refresh(
            Connection connection,
            TableIdentity tableId
    ) throws SQLException {
        TableDefinition dbTable = dialect.describeTable(connection, tableId);
        if (dbTable != null) {
            log.info("Refreshing metadata for table {} to {}", tableId, dbTable);
            cache.put(dbTable.id(), dbTable);
        } else {
            log.warn("Failed to refresh metadata for table {}", tableId);
        }
        return dbTable;
    }
}

