package com.databend.kafka.connect.sink;

import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;

import com.databend.kafka.connect.databendclient.ColumnIdentity;
import com.databend.kafka.connect.databendclient.SQLExpressionBuilder;
import com.databend.kafka.connect.util.DateTimeUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
public class TimestampIncrementingCriteria {

    /**
     * The values that can be used in a statement's WHERE clause.
     */
    public interface CriteriaValues {

        /**
         * Get the beginning of the time period.
         *
         * @return the beginning timestamp; may be null
         * @throws SQLException if there is a problem accessing the value
         */
        Timestamp beginTimestampValue() throws SQLException;

        /**
         * Get the end of the time period.
         *
         * @return the ending timestamp; never null
         * @throws SQLException if there is a problem accessing the value
         */
        Timestamp endTimestampValue() throws SQLException;

        /**
         * Get the last incremented value seen.
         *
         * @return the last incremented value from one of the rows
         * @throws SQLException if there is a problem accessing the value
         */
        Long lastIncrementedValue() throws SQLException;
    }

    protected static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final List<ColumnIdentity> timestampColumns;
    protected final ColumnIdentity incrementingColumn;
    protected final TimeZone timeZone;


    public TimestampIncrementingCriteria(
            ColumnIdentity incrementingColumn,
            List<ColumnIdentity> timestampColumns,
            TimeZone timeZone
    ) {
        this.timestampColumns =
                timestampColumns != null ? timestampColumns : Collections.<ColumnIdentity>emptyList();
        this.incrementingColumn = incrementingColumn;
        this.timeZone = timeZone;
    }

    protected boolean hasTimestampColumns() {
        return !timestampColumns.isEmpty();
    }

    protected boolean hasIncrementedColumn() {
        return incrementingColumn != null;
    }

    /**
     * Build the WHERE clause for the columns used in this criteria.
     *
     * @param builder the string builder to which the WHERE clause should be appended; never null
     */
    public void whereClause(SQLExpressionBuilder builder) {
        if (hasTimestampColumns() && hasIncrementedColumn()) {
            timestampIncrementingWhereClause(builder);
        } else if (hasTimestampColumns()) {
            timestampWhereClause(builder);
        } else if (hasIncrementedColumn()) {
            incrementingWhereClause(builder);
        }
    }

    /**
     * Set the query parameters on the prepared statement whose WHERE clause was generated with the
     * previous call to {@link #whereClause(SQLExpressionBuilder)}.
     *
     * @param stmt   the prepared statement; never null
     * @param values the values that can be used in the criteria parameters; never null
     * @throws SQLException if there is a problem using the prepared statement
     */
    public void setQueryParameters(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        if (hasTimestampColumns() && hasIncrementedColumn()) {
            setQueryParametersTimestampIncrementing(stmt, values);
        } else if (hasTimestampColumns()) {
            setQueryParametersTimestamp(stmt, values);
        } else if (hasIncrementedColumn()) {
            setQueryParametersIncrementing(stmt, values);
        }
    }

    protected void setQueryParametersTimestampIncrementing(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Timestamp beginTime = values.beginTimestampValue();
        Timestamp endTime = values.endTimestampValue();
        Long incOffset = values.lastIncrementedValue();
        stmt.setTimestamp(1, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setTimestamp(2, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setLong(3, incOffset);
        stmt.setTimestamp(4, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        log.debug(
                "Executing prepared statement with start time value = {} end time = {} and incrementing"
                        + " value = {}", DateTimeUtils.formatTimestamp(beginTime, timeZone),
                DateTimeUtils.formatTimestamp(endTime, timeZone), incOffset
        );
    }

    protected void setQueryParametersIncrementing(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Long incOffset = values.lastIncrementedValue();
        stmt.setLong(1, incOffset);
        log.debug("Executing prepared statement with incrementing value = {}", incOffset);
    }

    protected void setQueryParametersTimestamp(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Timestamp beginTime = values.beginTimestampValue();
        Timestamp endTime = values.endTimestampValue();
        stmt.setTimestamp(1, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setTimestamp(2, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        log.debug("Executing prepared statement with timestamp value = {} end time = {}",
                DateTimeUtils.formatTimestamp(beginTime, timeZone),
                DateTimeUtils.formatTimestamp(endTime, timeZone)
        );
    }






    protected Long extractDecimalId(Object incrementingColumnValue) {
        final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
        if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
            throw new ConnectException("Decimal value for incrementing column exceeded Long.MAX_VALUE");
        }
        if (decimal.scale() != 0) {
            throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
        }
        return decimal.longValue();
    }

    protected boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
        return incrementingColumnValue instanceof Long || incrementingColumnValue instanceof Integer
                || incrementingColumnValue instanceof Short || incrementingColumnValue instanceof Byte;
    }

    protected String coalesceTimestampColumns(SQLExpressionBuilder builder) {
        if (timestampColumns.size() == 1) {
            builder.append(timestampColumns.get(0));
        } else {
            builder.append("COALESCE(");
            builder.appendList().delimitedBy(",").of(timestampColumns);
            builder.append(")");
        }
        return builder.toString();
    }

    protected void timestampIncrementingWhereClause(SQLExpressionBuilder builder) {
        // This version combines two possible conditions. The first checks timestamp == last
        // timestamp and incrementing > last incrementing. The timestamp alone would include
        // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
        // get only the row with id = 23:
        //  timestamp 1234, id 22 <- last
        //  timestamp 1234, id 23
        // The second check only uses the timestamp > last timestamp. This covers everything new,
        // even if it is an update of the existing row. If we previously had:
        //  timestamp 1234, id 22 <- last
        // and then these rows were written:
        //  timestamp 1235, id 22
        //  timestamp 1236, id 23
        // We should capture both id = 22 (an update) and id = 23 (a new row)
        builder.append(" WHERE ");
        coalesceTimestampColumns(builder);
        builder.append(" < ? AND ((");
        coalesceTimestampColumns(builder);
        builder.append(" = ? AND ");
        builder.append(incrementingColumn);
        builder.append(" > ?");
        builder.append(") OR ");
        coalesceTimestampColumns(builder);
        builder.append(" > ?)");
        builder.append(" ORDER BY ");
        coalesceTimestampColumns(builder);
        builder.append(",");
        builder.append(incrementingColumn);
        builder.append(" ASC");
    }

    protected void incrementingWhereClause(SQLExpressionBuilder builder) {
        builder.append(" WHERE ");
        builder.append(incrementingColumn);
        builder.append(" > ?");
        builder.append(" ORDER BY ");
        builder.append(incrementingColumn);
        builder.append(" ASC");
    }

    protected void timestampWhereClause(SQLExpressionBuilder builder) {
        builder.append(" WHERE ");
        coalesceTimestampColumns(builder);
        builder.append(" > ? AND ");
        coalesceTimestampColumns(builder);
        builder.append(" < ? ORDER BY ");
        coalesceTimestampColumns(builder);
        builder.append(" ASC");
    }

    private List<String> findCaseSensitiveTimestampColumns(Schema schema) {
        Map<String, List<String>> caseInsensitiveColumns = schema.fields().stream()
                .map(Field::name)
                .collect(Collectors.groupingBy(String::toLowerCase));

        List<String> result = new ArrayList<>();
        for (ColumnIdentity timestampColumn : timestampColumns) {
            String columnName = timestampColumn.name();
            if (schema.field(columnName) != null) {
                log.trace(
                        "Timestamp column name {} case-sensitively matches column read from database",
                        columnName
                );
                result.add(columnName);
            } else {
                log.debug(
                        "Timestamp column name {} not found in columns read from database; "
                                + "falling back to a case-insensitive search",
                        columnName
                );
                List<String> caseInsensitiveMatches = caseInsensitiveColumns.get(columnName.toLowerCase());
                if (caseInsensitiveMatches == null || caseInsensitiveMatches.isEmpty()) {
                    throw new DataException("Timestamp column " + columnName + " not found in "
                            + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
                } else if (caseInsensitiveMatches.size() > 1) {
                    throw new DataException("Timestamp column " + columnName
                            + " not found in columns read from database: "
                            + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")) + ". "
                            + "Could not fall back to case-insensitively selecting a column "
                            + "because there were multiple columns whose names "
                            + "case-insensitively matched the specified name: "
                            + String.join(",", caseInsensitiveMatches) + ". "
                            + "To force the connector to choose between these columns, "
                            + "specify a value for the timestamp column configuration property "
                            + "that matches the desired column case-sensitively."
                    );
                } else {
                    String caseAdjustedColumnName = caseInsensitiveMatches.get(0);
                    log.debug(
                            "Falling back on column {} for user-specified timestamp column {} "
                                    + "(this is the only column that case-insensitively matches)",
                            caseAdjustedColumnName,
                            columnName
                    );
                    result.add(caseAdjustedColumnName);
                }
            }
        }

        return result;
    }

}

