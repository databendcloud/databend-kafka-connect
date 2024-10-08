package com.databend.kafka.connect.sink;

import com.databend.jdbc.DatabendPreparedStatement;
import com.databend.kafka.connect.databendclient.*;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SchemaPair;
import com.databend.kafka.connect.sink.metadata.SinkRecordField;
import com.databend.kafka.connect.util.DateTimeUtils;
import com.databend.kafka.connect.util.IdentifierRules;
import com.databend.kafka.connect.util.QuoteWay;
import com.databend.kafka.connect.databendclient.ColumnDefinition.Nullability;
import com.databend.kafka.connect.databendclient.ColumnDefinition.Mutability;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DatabendClient implements DatabendConnection {

    protected static final int NUMERIC_TYPE_SCALE_LOW = -84;
    protected static final int NUMERIC_TYPE_SCALE_HIGH = 127;
    protected static final int NUMERIC_TYPE_SCALE_UNSET = -127;

    // The maximum precision that can be achieved in a signed 64-bit integer is 2^63 ~= 9.223372e+18
    private static final int MAX_INTEGER_TYPE_PRECISION = 18;

    private static final String PRECISION_FIELD = "connect.decimal.precision";

    private static final Logger glog = LoggerFactory.getLogger(DatabendClient.class);

    protected final AbstractConfig config;

    protected String catalogPattern;
    protected final String databaseName;
    protected final Set<String> tableTypes;
    protected final String jdbcUrl;
    private final QuoteWay quoteSqlIdentifiers;
    private final IdentifierRules defaultIdentifierRules;
    private final AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
    private final Queue<Connection> connections = new ConcurrentLinkedQueue<>();
    private final int batchMaxRows;
    private final TimeZone timeZone;

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public static DatabendConnection create(AbstractConfig config) {
        return new DatabendClient(config);
    }

    public DatabendClient(AbstractConfig config) {
        this(config, IdentifierRules.DEFAULT);
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config                 the connector configuration; may not be null
     * @param defaultIdentifierRules the default rules for identifiers; may be null if the rules are
     *                               to be determined from the database metadata
     */
    protected DatabendClient(
            AbstractConfig config,
            IdentifierRules defaultIdentifierRules
    ) {
        this.config = config;
        DatabendSinkConfig sinkConfig = (DatabendSinkConfig) config;
        this.defaultIdentifierRules = defaultIdentifierRules;
        this.jdbcUrl = config.getString(DatabendSinkConfig.CONNECTION_URL);
        tableTypes = sinkConfig.tableTypeNames();
        catalogPattern = DatabendSinkConfig.CATALOG_PATTERN_DEFAULT;
        databaseName = config.getString(DatabendSinkConfig.DATABASE_CONFIG);
        quoteSqlIdentifiers = QuoteWay.get(
                config.getString(DatabendSinkConfig.QUOTE_SQL_IDENTIFIERS_CONFIG)
        );

        batchMaxRows = 0;

        if (config instanceof DatabendSinkConfig) {
            timeZone = ((DatabendSinkConfig) config).timeZone;
        } else {
            timeZone = TimeZone.getTimeZone(ZoneOffset.systemDefault());
        }
    }


    protected TimeZone timeZone() {
        return timeZone;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName("com.databend.jdbc.DatabendDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String username = config.getString(DatabendSinkConfig.CONNECTION_USER);
        Password dbPassword = config.getPassword(DatabendSinkConfig.CONNECTION_PASSWORD);
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (dbPassword != null) {
            properties.setProperty("password", dbPassword.value());
        }
        properties = addConnectionProperties(properties);
        // Timeout is 40 seconds to be as long as possible for customer to have a long connection
        // handshake, while still giving enough time to validate once in the follower worker,
        // and again in the leader worker and still be under 90s REST serving timeout
        DriverManager.setLoginTimeout(40);
        Connection connection = DriverManager.getConnection(jdbcUrl, properties);
        connections.add(connection);
        return connection;
    }

    @Override
    public void close() {
        Connection conn;
        while ((conn = connections.poll()) != null) {
            try {
                conn.close();
            } catch (Throwable e) {
                glog.warn("Error while closing connection to Databend", e);
            }
        }
    }

    @Override
    public boolean isConnectionValid(
            Connection connection,
            int timeout
    ) throws SQLException {
        String query = checkConnectionQuery();
        if (query != null) {
            try (Statement statement = connection.createStatement()) {
                if (statement.execute(query)) {
                    ResultSet rs = null;
                    try {
                        // do nothing with the result set
                        rs = statement.getResultSet();
                    } finally {
                        if (rs != null) {
                            rs.close();
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Return a query that can be used to check the validity of an existing database connection
     * when the Databend JDBC driver does not support JDBC 4. By default this returns {@code SELECT 1},
     * but subclasses should override this when a different query should be used.
     *
     * @return the check connection query; may be null if the connection should not be queried
     */
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }


    /**
     * Add or modify any connection properties based upon the {@link #config configuration}.
     *
     * <p>By default this method adds any {@code connection.*} properties (except those predefined
     * by the connector's ConfigDef, such as {@code connection.url}, {@code connection.user},
     * {@code connection.password}, {@code connection.attempts}, etc.) only after removing the
     * {@code connection.} prefix. This allows users to add any additional DBMS-specific properties
     * for the database to the connector configuration by prepending the DBMS-specific
     * properties with the {@code connection.} prefix.
     *
     * <p>Subclasses that don't wish to support this behavior can override this method without
     * calling this super method.
     *
     * @param properties the properties that will be passed to the {@link DriverManager}'s {@link
     *                   DriverManager#getConnection(String, Properties) getConnection(...) method};
     *                   never null
     * @return the updated connection properties, or {@code properties} if they are not modified or
     * should be returned; never null
     */
    protected Properties addConnectionProperties(Properties properties) {
        // Get the set of config keys that are known to the connector
        Set<String> configKeys = config.values().keySet();
        // Add any configuration property that begins with 'connection.` and that is not known
        // result should include props that began with 'connection.' but without prefix
        config.originalsWithPrefix(DatabendSinkConfig.CONNECTION_PREFIX).forEach((k, v) -> {
            if (!configKeys.contains(DatabendSinkConfig.CONNECTION_PREFIX + k)) {
                properties.put(k, v);
            }
        });
        return properties;
    }

    @Override
    public DatabendPreparedStatement createPreparedStatement(
            Connection db,
            String query
    ) throws SQLException {
        glog.trace("Creating a PreparedStatement '{}'", query);
        PreparedStatement stmt = db.prepareStatement(query);
        initializePreparedStatement(stmt);
        return (DatabendPreparedStatement) stmt;
    }

    /**
     * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
     * the {@link #createPreparedStatement(Connection, String)} method after the statement is
     * created but before it is returned/used.
     *
     * <p>By default this method sets the {@link PreparedStatement#setFetchSize(int) fetch size} to
     * the {@link DatabendSinkConfig#BATCH_MAX_ROWS_CONFIG batch size} of the connector.
     * This will provide a hint to the JDBC driver as to the number of rows to fetch from the database
     * in an attempt to limit memory usage when reading from large tables. Driver implementations
     * often require further configuration to make use of the fetch size.
     *
     * @param stmt the prepared statement; never null
     * @throws SQLException the error that might result from initialization
     */
    protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
        if (batchMaxRows > 0) {
            stmt.setFetchSize(batchMaxRows);
        }
    }

    @Override
    public TableIdentity parseTableIdentifier(String fqn) {
        List<String> parts = identifierRules().parseQualifiedIdentifier(fqn);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Invalid fully qualified name: '" + fqn + "'");
        }
        if (parts.size() == 1) {
            return new TableIdentity(null, null, parts.get(0));
        }
        if (parts.size() == 3) {
            return new TableIdentity(parts.get(0), parts.get(1), parts.get(2));
        }
        assert parts.size() >= 2;
        if (useCatalog()) {
            return new TableIdentity(parts.get(0), parts.get(0), parts.get(1));
        }
        return new TableIdentity(null, parts.get(0), parts.get(1));
    }

    /**
     * Return whether the database uses JDBC catalogs.
     *
     * @return true if catalogs are used, or false otherwise
     */
    protected boolean useCatalog() {
        return false;
    }

    @Override
    public List<TableIdentity> tableIds(Connection conn) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, ", ");
        glog.debug("Using {} dialect to get {}", this, tableTypeDisplay);

        try (ResultSet rs = metadata.getTables(catalogPattern(), getDatabaseName(), "%", null)) {
            List<TableIdentity> tableIds = new ArrayList<>();
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableIdentity tableId = new TableIdentity(catalogName, schemaName, tableName);
                if (includeTable(tableId)) {
                    tableIds.add(tableId);
                }
            }
            glog.debug("Used {} dialect to find {} {}", this, tableIds.size(), tableTypeDisplay);
            return tableIds;
        }
    }

    protected String catalogPattern() {
        return catalogPattern;
    }

    protected String getDatabaseName() {
        return databaseName;
    }

    /**
     * Determine whether the table with the specific name is to be included in the tables.
     *
     * <p>This method can be overridden to exclude certain database tables.
     *
     * @param table the identifier of the table; may be null
     * @return true if the table should be included; false otherwise
     */
    protected boolean includeTable(TableIdentity table) {
        return true;
    }

    /**
     * Find the available table types that are returned by the JDBC driver that case insensitively
     * match the specified types.
     *
     * @param metadata the database metadata; may not be null but may be empty if no table types
     * @param types    the case-independent table types that are desired
     * @return the array of table types take directly from the list of available types returned by the
     * JDBC driver; never null
     * @throws SQLException if there is an error with the database connection
     */
    protected String[] tableTypes(
            DatabaseMetaData metadata,
            Set<String> types
    ) throws SQLException {
        glog.debug("Using {} dialect to check support for {}", this, types);
        // Compute the uppercase form of the desired types ...
        Set<String> uppercaseTypes = new HashSet<>();
        for (String type : types) {
            if (type != null) {
                uppercaseTypes.add(type.toUpperCase(Locale.ROOT));
            }
        }
        // Now find out the available table types ...
        Set<String> matchingTableTypes = new HashSet<>();
        try (ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                String tableType = rs.getString(1);
                if (tableType != null) {
                    tableType = tableType.trim();
                    if (uppercaseTypes.contains(tableType.toUpperCase(Locale.ROOT))) {
                        matchingTableTypes.add(tableType);
                    }
                }
            }
        }
        String[] result = matchingTableTypes.toArray(new String[matchingTableTypes.size()]);
        glog.debug("Used {} dialect to find table types: {}", this, result);
        return result;
    }

    @Override
    public IdentifierRules identifierRules() {
        if (identifierRules.get() == null) {
            try (Connection connection = getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                String leadingQuoteStr = metaData.getIdentifierQuoteString();
                String trailingQuoteStr = leadingQuoteStr; // JDBC does not distinguish
                String separator = metaData.getCatalogSeparator();
                if (leadingQuoteStr == null || leadingQuoteStr.isEmpty()) {
                    leadingQuoteStr = defaultIdentifierRules.leadingQuoteString();
                    trailingQuoteStr = defaultIdentifierRules.trailingQuoteString();
                }
                if (separator == null || separator.isEmpty()) {
                    separator = defaultIdentifierRules.identifierDelimiter();
                }
                identifierRules.set(new IdentifierRules(separator, leadingQuoteStr, trailingQuoteStr));
            } catch (SQLException e) {
                if (defaultIdentifierRules != null) {
                    identifierRules.set(defaultIdentifierRules);
                    glog.warn("Unable to get identifier metadata; using default rules", e);
                } else {
                    throw new ConnectException("Unable to get identifier metadata", e);
                }
            }
        }
        return identifierRules.get();
    }

    @Override
    public SQLExpressionBuilder expressionBuilder() {
        return identifierRules().expressionBuilder()
                .setQuoteIdentifiers(quoteSqlIdentifiers);
    }

    /**
     * Return current time at the database
     *
     * @param conn database connection
     * @param cal  calendar
     * @return the current time at the database
     */
    @Override
    public Timestamp currentTimeOnDB(
            Connection conn,
            Calendar cal
    ) throws SQLException, ConnectException {
        String query = currentTimestampDatabaseQuery();
        assert query != null;
        assert !query.isEmpty();
        try (Statement stmt = conn.createStatement()) {
            glog.debug("executing query " + query + " to get current time from database");
            try (ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    return rs.getTimestamp(1, cal);
                } else {
                    throw new ConnectException(
                            "Unable to get current time from DB using " + this + " and query '" + query + "'"
                    );
                }
            }
        } catch (SQLException e) {
            glog.error("Failed to get current time from DB using {} and query '{}'", this, query, e);
            throw e;
        }
    }

    /**
     * Get the query string to determine the current timestamp in the database.
     *
     * @return the query string; never null or empty
     */
    protected String currentTimestampDatabaseQuery() {
        return "SELECT NOW()";
    }

    @Override
    public boolean tableExists(
            Connection connection,
            TableIdentity tableId
    ) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
        glog.info("Checking {} dialect for existence of {} {}", this, tableTypeDisplay, tableId);
        glog.info("catalogName is {}, schemaName is {}, tableName is {}", tableId.catalogName(), tableId.schemaName(), tableId.tableName());
        try (ResultSet rs = connection.getMetaData().getTables(
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                null
        )) {
            final boolean exists = rs.next();
            glog.info(
                    "Using {} dialect {} {} {} {}",
                    this,
                    tableTypeDisplay,
                    tableId.schemaName(),
                    tableId,
                    exists ? "present" : "absent"
            );
            return exists;
        }
    }


    protected String displayableTableTypes(String[] types, String delim) {
        return Arrays.stream(types).sorted().collect(Collectors.joining(delim));
    }

//    @Override
//    public Map<ColumnIdentity, ColumnDefinition> describeColumns(
//            Connection connection,
//            String tablePattern,
//            String columnPattern
//    ) throws SQLException {
//        //if the table pattern is fqn, then just use the actual table name
//        TableIdentity tableId = parseTableIdentifier(tablePattern);
//        String catalog = tableId.catalogName() != null ? tableId.catalogName() : catalogPattern;
//        String schema = tableId.schemaName() != null ? tableId.schemaName() : databaseName;
//        return describeColumns(connection, catalog, schema, tableId.tableName(), columnPattern);
//    }

    @Override
    public Map<ColumnIdentity, ColumnDefinition> describeColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern,
            String columnPattern
    ) throws SQLException {
        glog.debug(
                "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
                this,
                catalogPattern,
                schemaPattern,
                tablePattern
        );

        // Get the primary keys of the table(s) ...
        final Set<ColumnIdentity> pkColumns = primaryKeyColumns(
                connection,
                catalogPattern,
                schemaPattern,
                tablePattern,
                config.getList(DatabendSinkConfig.PK_FIELDS)
        );
        Map<ColumnIdentity, ColumnDefinition> results = new HashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                catalogPattern,
                schemaPattern,
                tablePattern,
                columnPattern
        )) {
            final int rsColumnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final TableIdentity tableId = new TableIdentity(catalogName, schemaName, tableName);
                final String columnName = rs.getString(4);
                final ColumnIdentity columnId = new ColumnIdentity(tableId, columnName, null);
                System.out.println(rs.getString(5));
                final String databendType = rs.getString(5).toLowerCase();
                final String typeName = rs.getString(6);
                final int precision = rs.getInt(7);
                final int scale = rs.getInt(9);
                final String typeClassName = null;
                Nullability nullability;
                final int nullableValue = rs.getInt("nullable");
                switch (nullableValue) {
                    case DatabaseMetaData.columnNoNulls:
                        nullability = Nullability.NOT_NULL;
                        break;
                    case DatabaseMetaData.columnNullable:
                        nullability = Nullability.NULL;
                        break;
                    case DatabaseMetaData.columnNullableUnknown:
                    default:
                        nullability = Nullability.UNKNOWN;
                        break;
                }
                Boolean autoIncremented = null;
                if (rsColumnCount >= 23) {
                    // Not all drivers include all columns ...
                    String isAutoIncremented = rs.getString(23);
                    if ("yes".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.TRUE;
                    } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.FALSE;
                    }
                }
                Boolean signed = null;
                Boolean caseSensitive = null;
                Boolean searchable = null;
                Boolean currency = null;
                Integer displaySize = null;
                boolean isPrimaryKey = pkColumns.contains(columnId);
                ColumnDefinition defn = columnDefinition(
                        rs,
                        columnId,
                        databendType,
                        typeName,
                        typeClassName,
                        nullability,
                        Mutability.UNKNOWN,
                        precision,
                        scale,
                        signed,
                        displaySize,
                        autoIncremented,
                        caseSensitive,
                        searchable,
                        currency,
                        isPrimaryKey
                );
                results.put(columnId, defn);
            }
            return results;
        }
    }

//    @Override
//    public Map<ColumnIdentity, ColumnDefinition> describeColumns(ResultSetMetaData rsMetadata) throws
//            SQLException {
//        Map<ColumnIdentity, ColumnDefinition> result = new LinkedHashMap<>();
//        for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
//            ColumnDefinition defn = describeColumn(rsMetadata, i);
//            result.put(defn.id(), defn);
//        }
//        return result;
//    }

//    /**
//     * Create a definition for the specified column in the result set.
//     *
//     * @param rsMetadata the result set metadata; may not be null
//     * @param column     the column number, starting at 1 for the first column
//     * @return the column definition; never null
//     * @throws SQLException if there is an error accessing the result set metadata
//     */
//    protected ColumnDefinition describeColumn(
//            ResultSetMetaData rsMetadata,
//            int column
//    ) throws SQLException {
//        String catalog = rsMetadata.getCatalogName(column);
//        String schema = rsMetadata.getSchemaName(column);
//        String tableName = rsMetadata.getTableName(column);
//        TableIdentity tableId = new TableIdentity(catalog, schema, tableName);
//        String name = rsMetadata.getColumnName(column);
//        String alias = rsMetadata.getColumnLabel(column);
//        ColumnIdentity id = new ColumnIdentity(tableId, name, alias);
//        Nullability nullability;
//        switch (rsMetadata.isNullable(column)) {
//            case ResultSetMetaData.columnNullable:
//                nullability = Nullability.NULL;
//                break;
//            case ResultSetMetaData.columnNoNulls:
//                nullability = Nullability.NOT_NULL;
//                break;
//            case ResultSetMetaData.columnNullableUnknown:
//            default:
//                nullability = Nullability.UNKNOWN;
//                break;
//        }
//        Mutability mutability = Mutability.MAYBE_WRITABLE;
//        if (rsMetadata.isReadOnly(column)) {
//            mutability = Mutability.READ_ONLY;
//        } else if (rsMetadata.isWritable(column)) {
//            mutability = Mutability.MAYBE_WRITABLE;
//        } else if (rsMetadata.isDefinitelyWritable(column)) {
//            mutability = Mutability.WRITABLE;
//        }
//        return new ColumnDefinition(
//                id,
//                rsMetadata.getColumnType(column),
//                rsMetadata.getColumnTypeName(column),
//                rsMetadata.getColumnClassName(column),
//                nullability,
//                mutability,
//                rsMetadata.getPrecision(column),
//                rsMetadata.getScale(column),
//                rsMetadata.isSigned(column),
//                rsMetadata.getColumnDisplaySize(column),
//                rsMetadata.isAutoIncrement(column),
//                rsMetadata.isCaseSensitive(column),
//                rsMetadata.isSearchable(column),
//                rsMetadata.isCurrency(column),
//                false
//        );
//    }

    protected Set<ColumnIdentity> primaryKeyColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern,
            List<String> pkFields
    ) throws SQLException {

        // Get the primary keys of the table(s) ...
        final Set<ColumnIdentity> pkColumns = new HashSet<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                catalogPattern, schemaPattern, tablePattern, null)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableIdentity tableId = new TableIdentity(catalogName, schemaName, tableName);
                final String colName = rs.getString(4);
                if (pkFields.contains(colName)) {
                    ColumnIdentity columnId = new ColumnIdentity(tableId, colName);
                    pkColumns.add(columnId);
                }
            }
        }
        return pkColumns;
    }

//    @Override
//    public Map<ColumnIdentity, ColumnDefinition> describeColumnsByQuerying(
//            Connection db,
//            TableIdentity tableId
//    ) throws SQLException {
//        String queryStr = "SELECT * FROM {} LIMIT 1";
//        String quotedName = expressionBuilder().append(tableId).toString();
//        try (PreparedStatement stmt = db.prepareStatement(queryStr)) {
//            stmt.setString(1, quotedName);
//            try (ResultSet rs = stmt.executeQuery()) {
//                ResultSetMetaData rsmd = rs.getMetaData();
//                return describeColumns(rsmd);
//            }
//        }
//    }

    @Override
    public TableDefinition describeTable(
            Connection connection,
            TableIdentity tableId
    ) throws SQLException {
        Map<ColumnIdentity, ColumnDefinition> columnDefns = describeColumns(connection, tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(), null
        );
        if (columnDefns.isEmpty()) {
            return null;
        }
        TableType tableType = tableTypeFor(connection, tableId);
        return new TableDefinition(tableId, columnDefns.values(), tableType);
    }

    protected TableType tableTypeFor(
            Connection connection,
            TableIdentity tableId
    ) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
        glog.info("Checking {} dialect for type of {} {} {}", this, tableTypeDisplay, tableId.schemaName(), tableId);
        try (ResultSet rs = connection.getMetaData().getTables(
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                tableTypes
        )) {
            if (rs.next()) {
                //final String catalogName = rs.getString(1);
                //final String schemaName = rs.getString(2);
                //final String tableName = rs.getString(3);
                String tableType = rs.getString(4);
                if (tableType.equalsIgnoreCase("base table")) {
                    tableType = "TABLE";
                }
                try {
                    return TableType.get(tableType);
                } catch (IllegalArgumentException e) {
                    glog.warn(
                            "{} dialect found unknown type '{}' for {} {}; using TABLE",
                            this,
                            tableType,
                            tableTypeDisplay,
                            tableId
                    );
                    return TableType.TABLE;
                }
            }
        }
        glog.warn(
                "{} dialect did not find type for {} {}; using TABLE",
                this,
                tableTypeDisplay,
                tableId
        );
        return TableType.TABLE;
    }

    /**
     * Create a ColumnDefinition with supplied values and the result set from the {@link
     * DatabaseMetaData#getColumns(String, String, String, String)} call. By default that method does
     * not describe whether the column is signed, case sensitive, searchable, currency, or the
     * preferred display size.
     *
     * <p>Subclasses can override this method to extract additional non-standard characteristics from
     * the result set, and override the characteristics determined using the standard JDBC metadata
     * columns and supplied as parameters.
     *
     * @param resultSet        the result set
     * @param id               the column identifier
     * @param databendType     the JDBC type of the column
     * @param typeName         the name of the column's type
     * @param classNameForType the name of the class used as instances of the value when {@link
     *                         ResultSet#getObject(int)} is called
     * @param nullability      the nullability of the column
     * @param mutability       the mutability of the column
     * @param precision        the precision of the column for numeric values, or the length for
     *                         non-numeric values
     * @param scale            the scale of the column for numeric values; ignored for other values
     * @param signedNumbers    true if the column holds signed numeric values; null if not known
     * @param displaySize      the preferred display size for the column values; null if not known
     * @param autoIncremented  true if the column is auto-incremented; null if not known
     * @param caseSensitive    true if the column values are case-sensitive; null if not known
     * @param searchable       true if the column is searchable; null if no; null if not known known
     * @param currency         true if the column is a currency value
     * @param isPrimaryKey     true if the column is part of the primary key; null if not known known
     * @return the column definition; never null
     */
    protected ColumnDefinition columnDefinition(
            ResultSet resultSet,
            ColumnIdentity id,
            String databendType,
            String typeName,
            String classNameForType,
            Nullability nullability,
            Mutability mutability,
            int precision,
            int scale,
            Boolean signedNumbers,
            Integer displaySize,
            Boolean autoIncremented,
            Boolean caseSensitive,
            Boolean searchable,
            Boolean currency,
            Boolean isPrimaryKey
    ) {
        return new ColumnDefinition(
                id,
                databendType,
                typeName,
                classNameForType,
                nullability,
                mutability,
                precision,
                scale,
                signedNumbers != null ? signedNumbers.booleanValue() : false,
                displaySize != null ? displaySize.intValue() : 0,
                autoIncremented != null ? autoIncremented.booleanValue() : false,
                caseSensitive != null ? caseSensitive.booleanValue() : false,
                searchable != null ? searchable.booleanValue() : false,
                currency != null ? currency.booleanValue() : false,
                isPrimaryKey != null ? isPrimaryKey.booleanValue() : false
        );
    }

    @Override
    public TimestampIncrementingCriteria criteriaFor(
            ColumnIdentity incrementingColumn,
            List<ColumnIdentity> timestampColumns
    ) {
        return new TimestampIncrementingCriteria(incrementingColumn, timestampColumns, timeZone);
    }

    /**
     * Determine the name of the field. By default this is the column alias or name.
     *
     * @param columnDefinition the column definition; never null
     * @return the field name; never null
     */
    protected String fieldNameFor(ColumnDefinition columnDefinition) {
        return columnDefinition.id().aliasOrName();
    }

    @Override
    public String addFieldToSchema(
            ColumnDefinition columnDefn,
            SchemaBuilder builder
    ) {
        return addFieldToSchema(columnDefn, builder, fieldNameFor(columnDefn), columnDefn.type(),
                columnDefn.isOptional()
        );
    }

    /**
     * Use the supplied {@link SchemaBuilder} to add a field that corresponds to the column with the
     * specified definition. This is intended to be easily overridden by subclasses.
     *
     * @param columnDefn   the definition of the column; may not be null
     * @param builder      the schema builder; may not be null
     * @param fieldName    the name of the field and {@link #fieldNameFor(ColumnDefinition) computed}
     *                     from the column definition; may not be null
     * @param databendType the JDBC {@link Types type} as obtained from the column definition
     * @param optional     true if the field is to be optional as obtained from the column definition
     * @return the name of the field, or null if no field was added
     */
    @SuppressWarnings("fallthrough")
    protected String addFieldToSchema(
            final ColumnDefinition columnDefn,
            final SchemaBuilder builder,
            final String fieldName,
            final String databendType,
            final boolean optional
    ) {
        int precision = columnDefn.precision();
        int scale = columnDefn.scale();
        switch (databendType) {
            case DatabendTypes.NULL: {
                glog.debug("JDBC type 'NULL' not currently supported for column '{}'", fieldName);
                return null;
            }

            case DatabendTypes.BOOLEAN: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA);
                break;
            }

            case DatabendTypes.INT8: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
                }
                break;
            }

            // 16 bit ints
            case DatabendTypes.INT16: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
                }
                break;
            }

            // 32 bit ints
            case DatabendTypes.INT32: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
                } else {
                    builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
                }
                break;
            }

            // 64 bit ints
            case DatabendTypes.INT64: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
                break;
            }

            case DatabendTypes.FLOAT32: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA);
                break;
            }

            case DatabendTypes.FLOAT64: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA);
                break;
            }

            case DatabendTypes.DECIMAL: {
                glog.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, scale);
                scale = decimalScale(columnDefn);
                SchemaBuilder fieldBuilder = Decimal.builder(scale);
                fieldBuilder.parameter(PRECISION_FIELD, Integer.toString(precision));
                if (optional) {
                    fieldBuilder.optional();
                }
                builder.field(fieldName, fieldBuilder.build());
                break;
            }

            case DatabendTypes.STRING: {
                builder.field(fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                break;
            }

            // Date is day + moth + year
            case DatabendTypes.DATE: {
                SchemaBuilder dateSchemaBuilder = org.apache.kafka.connect.data.Date.builder();
                if (optional) {
                    dateSchemaBuilder.optional();
                }
                builder.field(fieldName, dateSchemaBuilder.build());
                break;
            }


            // Timestamp is a date + time
            case DatabendTypes.TIMESTAMP: {
                SchemaBuilder timeSchemaBuilder = org.apache.kafka.connect.data.Time.builder();
                if (optional) {
                    timeSchemaBuilder.optional();
                }
                builder.field(fieldName, timeSchemaBuilder.build());
                break;
            }

            case DatabendTypes.ARRAY:
            case DatabendTypes.MAP:
            case DatabendTypes.TUPLE:
                builder.field(fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
                break;
            default: {
                glog.warn("Databend  ({}) not currently supported", columnDefn.typeName());
                return null;
            }
        }
        return fieldName;
    }

    private Schema integerSchema(boolean optional, int precision) {
        Schema schema;
        if (precision > 9) {
            schema = (optional) ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
        } else if (precision > 4) {
            schema = (optional) ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
        } else if (precision > 2) {
            schema = (optional) ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
        } else {
            schema = (optional) ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
        }
        return schema;
    }

    @Override
    public void applyDdlStatements(
            Connection connection,
            List<String> statements
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String ddlStatement : statements) {
                statement.execute(ddlStatement);
            }
        }
        try {
            connection.commit();
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                e.addSuppressed(sqle);
            }
        }
    }

    @Override
    public ColumnConverter createColumnConverter(
            ColumnMapping mapping
    ) {
        return columnConverterFor(mapping, mapping.columnDefn(), mapping.columnNumber(),
                true
        );
    }

    @SuppressWarnings({"deprecation", "fallthrough"})
    protected ColumnConverter columnConverterFor(
            final ColumnMapping mapping,
            final ColumnDefinition defn,
            final int col,
            final boolean isJdbc4
    ) {
        switch (mapping.columnDefn().type()) {

            case DatabendTypes.BOOLEAN: {
                return rs -> rs.getBoolean(col);
            }

            // 8 bits int
            case DatabendTypes.INT8: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getByte(col);
                } else {
                    return rs -> rs.getShort(col);
                }
            }

            // 16 bits int
            case DatabendTypes.INT16: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getShort(col);
                } else {
                    return rs -> rs.getInt(col);
                }
            }

            // 32 bits int
            case DatabendTypes.INT32: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getInt(col);
                } else {
                    return rs -> rs.getLong(col);
                }
            }

            // 64 bits int
            case DatabendTypes.INT64: {
                return rs -> rs.getLong(col);
            }


            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case DatabendTypes.FLOAT32:
                return rs -> rs.getFloat(col);
            case DatabendTypes.FLOAT64: {
                return rs -> rs.getDouble(col);
            }

            case DatabendTypes.DECIMAL: {
                int precision = defn.precision();
                glog.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, defn.scale());
                int scale = decimalScale(defn);
                return rs -> rs.getBigDecimal(col, scale);
            }

            case DatabendTypes.STRING:
                return rs -> rs.getString(col);


            // Date is day + month + year
            case DatabendTypes.DATE: {
                return rs -> rs.getDate(col,
                        DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC)));
            }

            // Timestamp is a date + time
            case DatabendTypes.TIMESTAMP:
                // Time is a time of day -- hour, minute, seconds, nanoseconds
                return rs -> rs.getTime(col, DateTimeUtils.getTimeZoneCalendar(timeZone));

            case DatabendTypes.ARRAY:
                return rs -> rs.getArray(col);
            case DatabendTypes.MAP:
            case DatabendTypes.TUPLE:
                return rs -> rs.getString(col);
            case DatabendTypes.NULL:
            default: {
                break;
            }
        }
        return null;
    }

    protected int decimalScale(ColumnDefinition defn) {
        return defn.scale() == NUMERIC_TYPE_SCALE_UNSET ? NUMERIC_TYPE_SCALE_HIGH : defn.scale();
    }

    /**
     * Called when the object has been fully read and {@link Blob#free()} should be called.
     *
     * @param blob the Blob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(Blob blob) throws SQLException {
        blob.free();
    }

    /**
     * Called when the object has been fully read and {@link Clob#free()} should be called.
     *
     * @param clob the Clob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(Clob clob) throws SQLException {
        clob.free();
    }

    @Override
    public String buildInsertStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    ) {
        SQLExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(SQLExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES (");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }


    @Override
    public String buildUpdateStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    ) {
        SQLExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(table);
        builder.append(" SET ");
        builder.appendList()
                .delimitedBy(", ")
                .transformedBy(SQLExpressionBuilder.columnNamesWith(" = ?"))
                .of(nonKeyColumns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                    .delimitedBy(" AND ")
                    .transformedBy(SQLExpressionBuilder.columnNamesWith(" = ?"))
                    .of(keyColumns);
        }
        return builder.toString();
    }

    /**
     * Return the transform that produces a prepared statement variable for each of the columns.
     * PostgreSQL may require the variable to have a type suffix, such as {@code ?::uuid}.
     *
     * @param defn the table definition; may be null if unknown
     * @return the transform that produces the variable expression for each column; never null
     */
    protected SQLExpressionBuilder.Transform<ColumnIdentity> columnValueVariables(TableDefinition defn) {
        return (builder, columnId) -> {
            builder.append("?");
            builder.append(valueTypeCast(defn, columnId));
        };
    }

    /**
     * Return the typecast expression that can be used as a suffix for a value variable of the
     * given column in the defined table.
     *
     * <p>This method returns a blank string except for those column types that require casting
     * when set with literal values. For example, a column of type {@code uuid} must be cast when
     * being bound with with a {@code varchar} literal, since a UUID value cannot be bound directly.
     *
     * @param tableDefn the table definition; may be null if unknown
     * @param columnId  the column within the table; may not be null
     * @return the cast expression, or an empty string; never null
     */
    protected String valueTypeCast(TableDefinition tableDefn, ColumnIdentity columnId) {
        if (tableDefn != null) {
            ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
            if (defn != null) {
                String typeName = defn.typeName(); // database-specific
                if (typeName != null) {
                    typeName = typeName.toLowerCase();
                }
            }
        }
        return "";
    }

    /*
     * sytax: REPLACE INTO <table_name> [ ( <col_name> [ , ... ] )  ON (<CONFLICT KEY>) ...
     *  "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " + ON (\"id\") VALUES (?,?,?,?) "
     * */
    @Override
    public String buildUpsertQueryStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    ) {
        SQLExpressionBuilder builder = expressionBuilder();
        builder.append("REPLACE INTO ");
        builder.append(table);
        builder.append(" (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(SQLExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") ON (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(SQLExpressionBuilder.columnNames())
                .of(keyColumns);
        builder.append(") VALUES (");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(this.columnValueVariables(definition))
                .of(keyColumns, nonKeyColumns);
        builder.append(")");
        return builder.toString();
    }


    @Override
    public final String buildDeleteStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns
    ) {
        SQLExpressionBuilder builder = expressionBuilder();
        builder.append("DELETE FROM ");
        builder.append(table);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                    .delimitedBy(" AND ")
                    .transformedBy(SQLExpressionBuilder.columnNamesWith(" = ?"))
                    .of(keyColumns);
        }
        return builder.toString();
    }

    @Override
    public void bindField(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value,
            ColumnDefinition colDef
    ) throws SQLException {
        if (value == null) {
            Integer type = getSqlTypeForSchema(schema);
            if (type != null) {
                statement.setNull(index, type);
            } else {
                statement.setObject(index, null);
            }
        } else {
            boolean bound = maybeBindLogical(statement, index, schema, value);
            if (!bound) {
                bound = maybeBindPrimitive(statement, index, schema, value);
            }
            if (!bound) {
                throw new ConnectException("Unsupported source data type: " + schema.type());
            }
        }
    }


    /**
     * Dialects not supporting `setObject(index, null)` can override this method
     * to provide a specific sqlType, as per the JDBC documentation
     * https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html
     *
     * @param schema the schema
     * @return the SQL type
     */
    protected Integer getSqlTypeForSchema(Schema schema) {
        return null;
    }

    protected boolean maybeBindPrimitive(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        switch (schema.type()) {
            case INT8:
                statement.setByte(index, (Byte) value);
                break;
            case INT16:
                statement.setShort(index, (Short) value);
                break;
            case INT32:
                statement.setInt(index, (Integer) value);
                break;
            case INT64:
                statement.setLong(index, (Long) value);
                break;
            case FLOAT32:
                statement.setFloat(index, (Float) value);
                break;
            case FLOAT64:
                statement.setDouble(index, (Double) value);
                break;
            case BOOLEAN:
                statement.setBoolean(index, (Boolean) value);
                break;
            case STRING:
                statement.setString(index, (String) value);
                break;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                statement.setBytes(index, bytes);
                break;
            default:
                return false;
        }
        return true;
    }

    protected boolean maybeBindLogical(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        if (schema.name() != null) {
            switch (schema.name()) {
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                    statement.setDate(
                            index,
                            new java.sql.Date(((java.util.Date) value).getTime())
                    );
                    return true;
                case Decimal.LOGICAL_NAME:
                    statement.setBigDecimal(index, (BigDecimal) value);
                    return true;
                case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
                    statement.setTime(
                            index,
                            new java.sql.Time(((java.util.Date) value).getTime())
                    );
                    return true;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    statement.setTimestamp(
                            index,
                            new java.sql.Timestamp(((java.util.Date) value).getTime())
                    );
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    @Override
    public String buildCreateTableStatement(
            TableIdentity table,
            Collection<SinkRecordField> fields
    ) {
        SQLExpressionBuilder builder = expressionBuilder();

        final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
        builder.append("CREATE TABLE IF NOT EXISTS ");
        builder.append(table);
        builder.append(" (");
        writeColumnsSpec(builder, fields);
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildDropTableStatement(
            TableIdentity table,
            DropOptions options
    ) {
        SQLExpressionBuilder builder = expressionBuilder();

        builder.append("DROP TABLE ");
        builder.append(table);
        if (options.ifExists()) {
            builder.append(" IF EXISTS");
        }
        if (options.cascade()) {
            builder.append(" CASCADE");
        }
        return builder.toString();
    }

    @Override
    public List<String> buildAlterTable(
            TableIdentity table,
            Collection<SinkRecordField> fields
    ) {
        final boolean newlines = fields.size() > 1;

        final SQLExpressionBuilder.Transform<SinkRecordField> transform = (builder, field) -> {
            if (newlines) {
                builder.appendNewLine();
            }
            builder.append("ADD COLUMN ");
            writeColumnSpec(builder, field);
        };

        SQLExpressionBuilder builder = expressionBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table);
        builder.append(" ");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(fields);
        return Collections.singletonList(builder.toString());
    }


    @Override
    public StatementBinder statementBinder(
            PreparedStatement statement,
            DatabendSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tableDefinition,
            DatabendSinkConfig.InsertMode insertMode
    ) {
        return new PreparedStatementBinder(
                this,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata,
                tableDefinition,
                insertMode
        );
    }

    @Override
    public void validateSpecificColumnTypes(
            ResultSetMetaData rsMetadata,
            List<ColumnIdentity> columns
    ) throws ConnectException {
    }


    protected List<String> extractPrimaryKeyFieldNames(Collection<SinkRecordField> fields) {
        final List<String> pks = new ArrayList<>();
        for (SinkRecordField f : fields) {
            if (f.isPrimaryKey()) {
                pks.add(f.name());
            }
        }
        return pks;
    }

    protected void writeColumnsSpec(
            SQLExpressionBuilder builder,
            Collection<SinkRecordField> fields
    ) {
        SQLExpressionBuilder.Transform<SinkRecordField> transform = (b, field) -> {
            b.append(System.lineSeparator());
            writeColumnSpec(b, field);
        };
        builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
    }

    protected void writeColumnSpec(
            SQLExpressionBuilder builder,
            SinkRecordField f
    ) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        String sqlType = getSqlType(f);
        builder.append(sqlType);
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            formatColumnValue(
                    builder,
                    f.schemaName(),
                    f.schemaParameters(),
                    f.schemaType(),
                    f.defaultValue()
            );
        } else if (isColumnOptional(f)) {
            builder.append(" NULL");
        } else {
            builder.append(" NOT NULL");
        }
    }

    protected boolean isColumnOptional(SinkRecordField field) {
        return field.isOptional();
    }

    protected void formatColumnValue(
            SQLExpressionBuilder builder,
            String schemaName,
            Map<String, String> schemaParameters,
            Schema.Type type,
            Object value
    ) {
        if (schemaName != null) {
            switch (schemaName) {
                case Decimal.LOGICAL_NAME:
                    builder.append(value);
                    return;
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone));
                    return;
                case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value, timeZone));
                    return;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    builder.appendStringQuoted(
                            DateTimeUtils.formatTimestamp((java.util.Date) value, timeZone)
                    );
                    return;
                default:
                    // fall through to regular types
                    break;
            }
        }
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                // no escaping required
                builder.append(value);
                break;
            case BOOLEAN:
                // 1 & 0 for boolean is more portable rather than TRUE/FALSE
                builder.append((Boolean) value ? '1' : '0');
                break;
            case STRING:
                builder.appendStringQuoted(value);
                break;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                builder.appendBinaryLiteral(bytes);
                break;
            default:
                throw new ConnectException("Unsupported type for column value: " + type);
        }
    }

    protected String getSqlTypeException(SinkRecordField f) {
        throw new ConnectException(String.format(
                "%s (%s) type doesn't have a mapping to the SQL database column type", f.schemaName(),
                f.schemaType()
        ));
    }

    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
                    return "TIME";
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "STRING";
            case MAP:
                return "STRING";
            case STRUCT:
                return "VARIANT";
            case ARRAY:
                SinkRecordField childField = new SinkRecordField(
                        field.schema().valueSchema(),
                        field.name(),
                        field.isPrimaryKey()
                );
                return getSqlType(childField) + "[]";
            default:
                return getSqlTypeException(field);
        }
    }

    /**
     * Return the sanitized form of the supplied JDBC URL, which masks any secrets or credentials.
     *
     * <p>This implementation replaces the value of all properties that contain {@code password}.
     *
     * @param url the JDBC URL; may not be null
     * @return the sanitized URL; never null
     */
    protected String sanitizedUrl(String url) {
        // Only replace standard URL-type properties ...
        return url.replaceAll("(?i)([?&]([^=&]*)password([^=&]*)=)[^&]*", "$1****");
    }

    @Override
    public String identifier() {
        return " database " + sanitizedUrl(jdbcUrl);
    }

    @Override
    public String toString() {
        return "Databend";
    }
}

