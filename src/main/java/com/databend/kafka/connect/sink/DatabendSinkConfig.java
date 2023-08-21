package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.util.DeleteEnabledRecommender;
import com.databend.kafka.connect.util.QuoteWay;
import com.databend.kafka.connect.databendclient.TableType;
import com.databend.kafka.connect.util.TimeZoneValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class DatabendSinkConfig extends AbstractConfig {

    public enum InsertMode {
        INSERT,
        UPSERT,
        UPDATE;

    }

    public enum PrimaryKeyMode {
        NONE,
        KAFKA,
        RECORD_KEY,
        RECORD_VALUE;
    }

    public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
            Arrays.asList(
                    "__connect_topic",
                    "__connect_partition",
                    "__connect_offset"
            )
    );

    public static final String CONNECTION_PREFIX = "connection.";

    public static final String CONNECTION_URL = CONNECTION_PREFIX + "url";
    private static final String CONNECTION_URL_DOC =
            "Databend JDBC connection URL.\n"
                    + "For example: ``jdbc:databend://root:root@localhost:8000``";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    public static final String CONNECTION_USER = CONNECTION_PREFIX + "user";
    private static final String CONNECTION_USER_DOC = "Databend connection user.";
    private static final String CONNECTION_USER_DISPLAY = "Databend User";

    public static final String CONNECTION_SSL = CONNECTION_PREFIX + "ssl";
    private static final String CONNECTION_SSL_DOC = "Databend connection ssl config.";
    private static final String CONNECTION_SSL_DISPLAY = "ssl true or false";

    public static final String CONNECTION_QUERY_TIMEOUT = CONNECTION_PREFIX + "query_timeout";
    private static final String CONNECTION_QUERY_TIMEOUT_DOC = "Databend queryTimeout config.";
    private static final String CONNECTION_QUERY_TIMEOUT_DISPLAY = "query timeout";

    public static final String CONNECTION_PASSWORD = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "Databend connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Databend Password";

    public static final String CONNECTION_ATTEMPTS = CONNECTION_PREFIX + "attempts";
    private static final String CONNECTION_ATTEMPTS_DOC = "Maximum number of attempts to retrieve a valid JDBC connection. "
            + "Must be a positive integer.";
    private static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

    public static final String CONNECTION_BACKOFF = CONNECTION_PREFIX + "backoff.ms";
    private static final String CONNECTION_BACKOFF_DOC =
            "Backoff time in milliseconds between connection attempts.";
    private static final String CONNECTION_BACKOFF_DISPLAY =
            "Databend connection backoff in milliseconds";
    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

    public static final String TABLE_NAME_FORMAT = "table.name.format";
    private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
    private static final String TABLE_NAME_FORMAT_DOC =
            "A format string for the destination table name, which may contain '${topic}' as a "
                    + "placeholder for the originating topic name.\n"
                    + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name "
                    + "'kafka_orders'.";
    private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 3000;
    private static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination"
                    + " table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";

    public static final String DELETE_ENABLED = "delete.enabled";
    private static final String DELETE_ENABLED_DEFAULT = "false";
    private static final String DELETE_ENABLED_DOC =
            "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
                    + "to be ``record_key``.";
    private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

    public static final String AUTO_CREATE = "auto.create";
    private static final String AUTO_CREATE_DEFAULT = "false";
    private static final String AUTO_CREATE_DOC =
            "Whether to automatically create the destination table based on record schema if it is "
                    + "found to be missing by issuing ``CREATE``.";
    private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

    public static final String AUTO_EVOLVE = "auto.evolve";
    private static final String AUTO_EVOLVE_DEFAULT = "false";
    private static final String AUTO_EVOLVE_DOC =
            "Whether to automatically add columns in the table schema when found to be missing relative "
                    + "to the record schema by issuing ``ALTER``.";
    private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

    public static final String INSERT_MODE = "insert.mode";
    private static final String INSERT_MODE_DEFAULT = "insert";
    private static final String INSERT_MODE_DOC =
            "The insertion mode to use. Supported modes are:\n"
                    + "``insert``\n"
                    + "    Use standard SQL ``INSERT`` statements.\n"
                    + "``upsert``\n"
                    + "    Use the appropriate upsert semantics for the target database if it is supported by "
                    + "the connector, e.g. ``INSERT OR IGNORE``.\n"
                    + "``update``\n"
                    + "    Use the appropriate update semantics for the target database if it is supported by "
                    + "the connector, e.g. ``UPDATE``.";
    private static final String INSERT_MODE_DISPLAY = "Insert Mode";

    public static final String PK_FIELDS = "pk.fields";
    private static final String PK_FIELDS_DEFAULT = "";
    private static final String PK_FIELDS_DOC =
            "List of comma-separated primary key field names."
                    + "    If empty, all fields from the value struct will be used, otherwise used to extract "
                    + "the desired fields.";
    private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    public static final String PK_MODE = "pk.mode";
    private static final String PK_MODE_DEFAULT = "none";
    private static final String PK_MODE_DOC =
            "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
                    + "Supported modes are:\n"
                    + "``none``\n"
                    + "    No keys utilized.\n"
                    + "``kafka``\n"
                    + "    Kafka coordinates are used as the PK.\n"
                    + "``record_key``\n"
                    + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
                    + "``record_value``\n"
                    + "    Field(s) from the record value are used, which must be a struct.";
    private static final String PK_MODE_DISPLAY = "Primary Key Mode";

    public static final String FIELDS_WHITELIST = "fields.whitelist";
    private static final String FIELDS_WHITELIST_DEFAULT = "";
    private static final String FIELDS_WHITELIST_DOC =
            "List of comma-separated record value field names. If empty, all fields from the record "
                    + "value are utilized, otherwise used to filter to the desired fields.\n"
                    + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
                    + "(s) form the primary key columns in the destination database,"
                    + " while this configuration is applicable for the other columns.";
    private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    private static final String CONNECTION_GROUP = "Connection";
    private static final String WRITES_GROUP = "Writes";
    private static final String DATAMAPPING_GROUP = "Data Mapping";
    private static final String DDL_GROUP = "DDL Support";
    private static final String DML_GROUP = "DML Support";
    private static final String RETRIES_GROUP = "Retries";

    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    public static final String DB_TIMEZONE_DEFAULT = "UTC";
    private static final String DB_TIMEZONE_CONFIG_DOC =
            "Name of the Databend JDBC timezone that should be used in the connector when "
                    + "inserting time-based values. Defaults to UTC.";
    private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";

    public static final String TABLE_TYPES_CONFIG = "table.types";
    private static final String TABLE_TYPES_DISPLAY = "Table Types";
    public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
    private static final String TABLE_TYPES_DOC =
            "The comma-separated types of database tables to which the sink connector can write. "
                    + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
                    + TableType.TABLE + "``, `` and ``"
                    + TableType.VIEW + "`` is allowed.";

    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
    public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteWay.ALWAYS.name().toString();
    public static final String QUOTE_SQL_IDENTIFIERS_DOC =
            "When to quote table names, column names, and other identifiers in SQL statements. "
                    + "For backward compatibility, the default is ``always``.";
    public static final String QUOTE_SQL_IDENTIFIERS_DISPLAY = "Quote Identifiers";

    public static final String TRIM_SENSITIVE_LOG_ENABLED = "trim.sensitive.log";
    private static final String TRIM_SENSITIVE_LOG_ENABLED_DEFAULT = "false";

    public static final String DATABASE_CONFIG = "database";
    private static final String DATABASE_DOC =
            "Database  to fetch table metadata from the database.\n"
                    + "   null indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the schema.";
    private static final String DATABASE_DISPLAY = "database name";
    public static final String DATABASE_DEFAULT = "default";

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";
    private static final String CATALOG_PATTERN_DOC =
            "Catalog pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a catalog \n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the catalog.";
    private static final String CATALOG_PATTERN_DISPLAY = "Schema pattern";
    public static final String CATALOG_PATTERN_DEFAULT = "default";
    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connection
            .define(
                    CONNECTION_URL,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_URL_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    CONNECTION_URL_DISPLAY
            )
            .define(
                    CONNECTION_USER,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_USER_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_USER_DISPLAY
            )
            .define(
                    CONNECTION_SSL,
                    ConfigDef.Type.BOOLEAN,
                    null,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_SSL_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_SSL_DISPLAY
            )
            .define(
                    CONNECTION_QUERY_TIMEOUT,
                    ConfigDef.Type.INT,
                    120,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_QUERY_TIMEOUT_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_QUERY_TIMEOUT_DISPLAY
            )
            .define(
                    CONNECTION_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_PASSWORD_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_PASSWORD_DISPLAY
            )
            .define(
                    CONNECTION_ATTEMPTS,
                    ConfigDef.Type.INT,
                    CONNECTION_ATTEMPTS_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    CONNECTION_ATTEMPTS_DOC,
                    CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    CONNECTION_ATTEMPTS_DISPLAY
            ).define(
                    CONNECTION_BACKOFF,
                    ConfigDef.Type.LONG,
                    CONNECTION_BACKOFF_DEFAULT,
                    ConfigDef.Importance.LOW,
                    CONNECTION_BACKOFF_DOC,
                    CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    CONNECTION_BACKOFF_DISPLAY
            )
            // Writes
            .define(
                    INSERT_MODE,
                    ConfigDef.Type.STRING,
                    INSERT_MODE_DEFAULT,
                    EnumValidator.in(InsertMode.values()),
                    ConfigDef.Importance.HIGH,
                    INSERT_MODE_DOC,
                    WRITES_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    INSERT_MODE_DISPLAY
            )
            .define(
                    BATCH_SIZE,
                    ConfigDef.Type.INT,
                    BATCH_SIZE_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    BATCH_SIZE_DOC, WRITES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    BATCH_SIZE_DISPLAY
            )
            .define(
                    DELETE_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    DELETE_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    DELETE_ENABLED_DOC, WRITES_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    DELETE_ENABLED_DISPLAY,
                    DeleteEnabledRecommender.INSTANCE
            )
            // Data Mapping
            .define(
                    TABLE_NAME_FORMAT,
                    ConfigDef.Type.STRING,
                    TABLE_NAME_FORMAT_DEFAULT,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    TABLE_NAME_FORMAT_DOC,
                    DATAMAPPING_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    TABLE_NAME_FORMAT_DISPLAY
            )
            .define(
                    PK_FIELDS,
                    ConfigDef.Type.LIST,
                    PK_FIELDS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    PK_FIELDS_DOC,
                    DATAMAPPING_GROUP,
                    3,
                    ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
            )
            .define(
                    FIELDS_WHITELIST,
                    ConfigDef.Type.LIST,
                    FIELDS_WHITELIST_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    FIELDS_WHITELIST_DOC,
                    DATAMAPPING_GROUP,
                    4,
                    ConfigDef.Width.LONG,
                    FIELDS_WHITELIST_DISPLAY
            ).define(
                    DB_TIMEZONE_CONFIG,
                    ConfigDef.Type.STRING,
                    DB_TIMEZONE_DEFAULT,
                    TimeZoneValidator.INSTANCE,
                    ConfigDef.Importance.MEDIUM,
                    DB_TIMEZONE_CONFIG_DOC,
                    DATAMAPPING_GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    DB_TIMEZONE_CONFIG_DISPLAY
            )
            // DDL
            .define(
                    AUTO_CREATE,
                    ConfigDef.Type.BOOLEAN,
                    AUTO_CREATE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    AUTO_CREATE_DOC, DDL_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    AUTO_CREATE_DISPLAY
            )
            .define(
                    AUTO_EVOLVE,
                    ConfigDef.Type.BOOLEAN,
                    AUTO_EVOLVE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    AUTO_EVOLVE_DOC, DDL_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    AUTO_EVOLVE_DISPLAY
            )
            // Retries
            .define(
                    MAX_RETRIES,
                    ConfigDef.Type.INT,
                    MAX_RETRIES_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRIES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRIES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY
            )
            .defineInternal(
                    TRIM_SENSITIVE_LOG_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    TRIM_SENSITIVE_LOG_ENABLED_DEFAULT,
                    ConfigDef.Importance.LOW
            );

    public final String connectionUrl;
    public final String connectionUser;
    public final Boolean ssl;
    public final Integer queryTimeout;
    public final String connectionPassword;
    public final int connectionAttempts;
    public final long connectionBackoffMs;
    public final String tableNameFormat;
    public final int batchSize;
    public final boolean deleteEnabled;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final boolean autoCreate;
    public final boolean autoEvolve;
    public final InsertMode insertMode;
    public final List<String> pkFields;
    public final PrimaryKeyMode pkMode;
    public final Set<String> fieldsWhitelist;
    public final TimeZone timeZone;
    public final EnumSet<TableType> tableTypes;

    public final boolean trimSensitiveLogsEnabled;

    public DatabendSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
        connectionUrl = getString(CONNECTION_URL);
        connectionUser = getString(CONNECTION_USER);
        ssl = getBoolean(CONNECTION_SSL);
        queryTimeout = getInt(CONNECTION_QUERY_TIMEOUT);
        connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
        connectionAttempts = getInt(CONNECTION_ATTEMPTS);
        connectionBackoffMs = getLong(CONNECTION_BACKOFF);
        tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
        batchSize = getInt(BATCH_SIZE);
        deleteEnabled = getBoolean(DELETE_ENABLED);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        autoCreate = getBoolean(AUTO_CREATE);
        autoEvolve = getBoolean(AUTO_EVOLVE);
        insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
        pkFields = getList(PK_FIELDS);
        fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
        String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
        timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));
        trimSensitiveLogsEnabled = getBoolean(TRIM_SENSITIVE_LOG_ENABLED);
    }

    private String getPasswordValue(String key) {
        Password password = getPassword(key);
        if (password != null) {
            return password.value();
        }
        return null;
    }

    public EnumSet<TableType> tableTypes() {
        return tableTypes;
    }

    public Set<String> tableTypeNames() {
        return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
    }

    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(String key, Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }

}
