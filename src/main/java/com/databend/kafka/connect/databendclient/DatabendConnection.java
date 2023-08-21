package com.databend.kafka.connect.databendclient;

import com.databend.kafka.connect.sink.DatabendSinkConfig;
import com.databend.kafka.connect.sink.DatabendSinkTask;
import com.databend.kafka.connect.sink.TimestampIncrementingCriteria;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SchemaPair;
import com.databend.kafka.connect.sink.metadata.SinkRecordField;
import com.databend.kafka.connect.util.IdentifierRules;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DatabendConnection extends ConnectionProvider {

    /**
     * Create a new prepared statement using the specified database connection.
     *
     * @param connection the database connection; may not be null
     * @param query      the query expression for the prepared statement; may not be null
     * @return a new prepared statement; never null
     * @throws SQLException if there is an error with the database connection
     */
    PreparedStatement createPreparedStatement(
            Connection connection,
            String query
    ) throws SQLException;

    /**
     * Parse the supplied simple name or fully qualified name for a table into a {@link TableIdentity}.
     *
     * @param fqn the fully qualified string representation; may not be null
     * @return the table identifier; never null
     */
    TableIdentity parseTableIdentifier(String fqn);

    /**
     * Get the identifier rules for this database.
     *
     * @return the identifier rules
     */
    IdentifierRules identifierRules();

    /**
     * Get a new {@link SQLExpressionBuilder} that can be used to build expressions with quoted
     * identifiers.
     *
     * @return the builder; never null
     * @see #identifierRules()
     * @see IdentifierRules#expressionBuilder()
     */
    SQLExpressionBuilder expressionBuilder();

    /**
     * Return current time at the database
     *
     * @param connection database connection
     * @param cal        calendar
     * @return the current time at the database
     * @throws SQLException if there is an error with the database connection
     */
    Timestamp currentTimeOnDB(
            Connection connection,
            Calendar cal
    ) throws SQLException, ConnectException;

    /**
     * Get a list of identifiers of the non-system tables in the database.
     *
     * @param connection database connection
     * @return a list of tables; never null
     * @throws SQLException if there is an error with the database connection
     */
    List<TableIdentity> tableIds(Connection connection) throws SQLException;

    /**
     * Determine if the specified table exists in the database.
     *
     * @param connection the database connection; may not be null
     * @param tableId    the identifier of the table; may not be null
     * @return true if the table exists, or false otherwise
     * @throws SQLException if there is an error accessing the metadata
     */
    boolean tableExists(Connection connection, TableIdentity tableId) throws SQLException;

//    /**
//     * Create the definition for the columns described by the database metadata using the current
//     * schema and catalog patterns defined in the configuration.
//     *
//     * @param connection    the database connection; may not be null
//     * @param tablePattern  the pattern for matching the tables; may be null
//     * @param columnPattern the pattern for matching the columns; may be null
//     * @return the column definitions keyed by their {@link ColumnIdentity}; never null
//     * @throws SQLException if there is an error accessing the metadata
//     */
//    Map<ColumnIdentity, ColumnDefinition> describeColumns(
//            Connection connection,
//            String tablePattern,
//            String columnPattern
//    ) throws SQLException;

    /**
     * Create the definition for the columns described by the database metadata.
     *
     * @param connection     the database connection; may not be null
     * @param catalogPattern the pattern for matching the catalog; may be null
     * @param schemaPattern  the pattern for matching the schemas; may be null
     * @param tablePattern   the pattern for matching the tables; may be null
     * @param columnPattern  the pattern for matching the columns; may be null
     * @return the column definitions keyed by their {@link ColumnIdentity}; never null
     * @throws SQLException if there is an error accessing the metadata
     */
    Map<ColumnIdentity, ColumnDefinition> describeColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern,
            String columnPattern
    ) throws SQLException;

//    /**
//     * Create the definition for the columns in the result set.
//     *
//     * @param rsMetadata the result set metadata; may not be null
//     * @return the column definitions keyed by their {@link ColumnIdentity} and in the same order as the
//     * result set; never null
//     * @throws SQLException if there is an error accessing the result set metadata
//     */
//    Map<ColumnIdentity, ColumnDefinition> describeColumns(
//            ResultSetMetaData rsMetadata
//    ) throws SQLException;

    /**
     * Get the definition of the specified table.
     *
     * @param connection the database connection; may not be null
     * @param tableId    the identifier of the table; may not be null
     * @return the table definition; null if the table does not exist
     * @throws SQLException if there is an error accessing the metadata
     */
    TableDefinition describeTable(Connection connection, TableIdentity tableId) throws SQLException;

//    /**
//     * Create the definition for the columns in the result set returned when querying the table. This
//     * may not work if the table is empty.
//     *
//     * @param connection the database connection; may not be null
//     * @param tableId    the name of the table; may be null
//     * @return the column definitions keyed by their {@link ColumnIdentity}; never null
//     * @throws SQLException if there is an error accessing the result set metadata
//     */
//    Map<ColumnIdentity, ColumnDefinition> describeColumnsByQuerying(
//            Connection connection,
//            TableIdentity tableId
//    ) throws SQLException;

    /**
     * Create a criteria generator for queries that look for changed data using timestamp and
     * incremented columns.
     *
     * @param incrementingColumn the identifier of the incremented column; may be null if there is
     *                           none
     * @param timestampColumns   the identifiers of the timestamp column; may be null if there is
     *                           none
     * @return the {@link TimestampIncrementingCriteria} implementation; never null
     */
    TimestampIncrementingCriteria criteriaFor(
            ColumnIdentity incrementingColumn,
            List<ColumnIdentity> timestampColumns
    );

    /**
     * Use the supplied {@link SchemaBuilder} to add a field that corresponds to the column with the
     * specified definition.
     *
     * @param column  the definition of the column; may not be null
     * @param builder the schema builder; may not be null
     * @return the name of the field, or null if no field was added
     */
    String addFieldToSchema(ColumnDefinition column, SchemaBuilder builder);

    /**
     * Apply the supplied DDL statements using the given connection. This gives the dialect the
     * opportunity to execute the statements with a different autocommit setting.
     *
     * @param connection the connection to use
     * @param statements the list of DDL statements to execute
     * @throws SQLException if there is an error executing the statements
     */
    void applyDdlStatements(Connection connection, List<String> statements) throws SQLException;

    /**
     * Build the INSERT prepared statement expression for the given table and its columns.
     *
     * @param table         the identifier of the table; may not be null
     * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
     *                      but may be empty
     * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
     *                      be empty
     * @param definition    the table definition; may be null if unknown
     * @return the INSERT statement; may not be null
     */
    String buildInsertStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    );

    /**
     * Build the UPDATE prepared statement expression for the given table and its columns. Variables
     * for each key column should also appear in the WHERE clause of the statement.
     *
     * @param table         the identifier of the table; may not be null
     * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
     *                      but may be empty
     * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
     *                      be empty
     * @param definition    the table definition; may be null if unknown
     * @return the UPDATE statement; may not be null
     */
    String buildUpdateStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    );


    /**
     * Build the UPSERT or MERGE prepared statement expression to either insert a new record into the
     * given table or update an existing record in that table Variables for each key column should
     * also appear in the WHERE clause of the statement.
     *
     * @param table         the identifier of the table; may not be null
     * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
     *                      but may be empty
     * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
     *                      be empty
     * @param definition    the table definition; may be null if unknown
     * @return the upsert/merge statement; may not be null
     * @throws UnsupportedOperationException if the dialect does not support upserts
     */
    String buildUpsertQueryStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns,
            Collection<ColumnIdentity> nonKeyColumns,
            TableDefinition definition
    );

    /**
     * Build the DELETE prepared statement expression for the given table and its columns. Variables
     * for each key column should also appear in the WHERE clause of the statement.
     *
     * @param table      the identifier of the table; may not be null
     * @param keyColumns the identifiers of the columns in the primary/unique key; may not be null
     *                   but may be empty
     * @return the delete statement; may not be null
     * @throws UnsupportedOperationException if the dialect does not support deletes
     */
    default String buildDeleteStatement(
            TableIdentity table,
            Collection<ColumnIdentity> keyColumns
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Build the DROP TABLE statement expression for the given table.
     *
     * @param table   the identifier of the table; may not be null
     * @param options the options; may be null
     * @return the DROP TABLE statement; may not be null
     */
    String buildDropTableStatement(TableIdentity table, DropOptions options);

    /**
     * Build the CREATE TABLE statement expression for the given table and its columns.
     *
     * @param table  the identifier of the table; may not be null
     * @param fields the information about the fields in the sink records; may not be null
     * @return the CREATE TABLE statement; may not be null
     */
    String buildCreateTableStatement(TableIdentity table, Collection<SinkRecordField> fields);

    /**
     * Build the ALTER TABLE statement expression for the given table and its columns.
     *
     * @param table  the identifier of the table; may not be null
     * @param fields the information about the fields in the sink records; may not be null
     * @return the ALTER TABLE statement; may not be null
     */
    List<String> buildAlterTable(TableIdentity table, Collection<SinkRecordField> fields);

    /**
     * Create a component that can bind record values into the supplied prepared statement.
     *
     * @param statement       the prepared statement
     * @param pkMode          the primary key mode; may not be null
     * @param schemaPair      the key and value schemas; may not be null
     * @param fieldsMetadata  the field metadata; may not be null
     * @param tableDefinition the table definition; may be null
     * @param insertMode      the insert mode; may not be null
     * @return the statement binder; may not be null
     */
    StatementBinder statementBinder(
            PreparedStatement statement,
            DatabendSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tableDefinition,
            DatabendSinkConfig.InsertMode insertMode
    );

    /**
     * @param rsMetadata the result set metadata; may not be null
     * @param columns    columns to check; may not be null
     * @throws ConnectException if column type not compatible with connector
     *                          or if there is an error accessing the result set metadata
     */
    void validateSpecificColumnTypes(
            ResultSetMetaData rsMetadata,
            List<ColumnIdentity> columns
    ) throws ConnectException;


    /**
     * Method that binds a value with the given schema at the specified variable within a prepared
     * statement.
     *
     * @param statement the prepared statement; may not be null
     * @param index     the 1-based index of the variable within the prepared statement
     * @param schema    the schema for the value; may be null only if the value is null
     * @param value     the value to be bound to the variable; may be null
     * @param colDef    the Definition of the column to be bound; may be null
     * @throws SQLException if there is a problem binding the value into the statement
     */
    void bindField(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value,
            ColumnDefinition colDef
    ) throws SQLException;

    /**
     * A function to bind the values from a sink record into a prepared statement.
     */
    @FunctionalInterface
    interface StatementBinder {

        /**
         * Bind the values in the supplied record.
         *
         * @param record the sink record with values to be bound into the statement; never null
         * @throws SQLException if there is a problem binding values into the statement
         */
        void bindRecord(SinkRecord record) throws SQLException;
    }

    /**
     * Create a function that converts column values for the column defined by the specified mapping.
     *
     * @param mapping the column definition and the corresponding {@link Field}; may not be null
     * @return the column converter function; or null if the column should be ignored
     */
    ColumnConverter createColumnConverter(ColumnMapping mapping);

    /**
     * A function that obtains a column value from the current row of the specified result set.
     */
    @FunctionalInterface
    interface ColumnConverter {

        /**
         * Get the column's value from the row at the current position in the result set, and convert it
         * to a value that should be included in the corresponding {@link Field} in the {@link org.apache.kafka.connect.data.Struct}
         * for the row.
         *
         * @param resultSet the result set; never null
         * @return the value of the {@link Field} as converted from the column value
         * @throws SQLException if there is an error with the database connection
         * @throws IOException  if there is an error accessing a streaming value from the result set
         */
        Object convert(ResultSet resultSet) throws SQLException, IOException;
    }
}

