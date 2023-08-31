package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.*;
import com.databend.kafka.connect.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabendConnectionTest {
    protected TableIdentity tableId;
    protected ColumnIdentity columnPK1;
    protected ColumnIdentity columnPK2;
    protected ColumnIdentity columnA;
    protected ColumnIdentity columnB;
    protected ColumnIdentity columnC;
    protected ColumnIdentity columnD;
    protected List<ColumnIdentity> pkColumns;
    protected List<ColumnIdentity> columnsAtoD;
    protected List<SinkRecordField> sinkRecordFields;
    protected DatabendConnection dialect;
    protected int defaultLoginTimeout;
    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
    protected static final GregorianCalendar MARCH_15_2001_MIDNIGHT;
    public final DatabendHelper databendHelper = new DatabendHelper(getClass().getSimpleName());

    static {
        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);

        MARCH_15_2001_MIDNIGHT = new GregorianCalendar(2001, Calendar.MARCH, 15, 0, 0, 0);
        MARCH_15_2001_MIDNIGHT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Before
    public void setup() throws Exception {
        defaultLoginTimeout = DriverManager.getLoginTimeout();
        DriverManager.setLoginTimeout(1);


        // Set up some data ...
        Schema optionalDateWithDefault = Date.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalTimeWithDefault = Time.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalTsWithDefault = Timestamp.builder()
                .defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalDecimal = Decimal.builder(4).optional().parameter("p1", "v1")
                .parameter("p2", "v2").build();
        Schema booleanWithDefault = SchemaBuilder.bool().defaultValue(true);
        tableId = new TableIdentity(null, null, "myTable");
        columnPK1 = new ColumnIdentity(tableId, "id1");
        columnPK2 = new ColumnIdentity(tableId, "id2");
        columnA = new ColumnIdentity(tableId, "columnA");
        columnB = new ColumnIdentity(tableId, "columnB");
        columnC = new ColumnIdentity(tableId, "columnC");
        columnD = new ColumnIdentity(tableId, "columnD");
        pkColumns = Arrays.asList(columnPK1, columnPK2);
        columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);

        SinkRecordField f1 = new SinkRecordField(Schema.INT32_SCHEMA, "c1", true);
        SinkRecordField f2 = new SinkRecordField(Schema.INT64_SCHEMA, "c2", false);
        SinkRecordField f3 = new SinkRecordField(Schema.STRING_SCHEMA, "c3", false);
        SinkRecordField f4 = new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "c4", false);
        SinkRecordField f5 = new SinkRecordField(optionalDateWithDefault, "c5", false);
        SinkRecordField f6 = new SinkRecordField(optionalTimeWithDefault, "c6", false);
        SinkRecordField f7 = new SinkRecordField(optionalTsWithDefault, "c7", false);
        SinkRecordField f8 = new SinkRecordField(optionalDecimal, "c8", false);
        SinkRecordField f9 = new SinkRecordField(booleanWithDefault, "c9", false);
        sinkRecordFields = Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9);

        Map<Object, Object> props = new HashMap<>();
        props.put("name", "databend-connector");
        props.put("connection.url", databendHelper.databendUri());
        props.put("batch.size", String.valueOf(1000)); // sufficiently high to not cause flushes due to buffer being full
        // We don't manually create the table, so let the connector do it
        props.put("auto.create", true);
        // We use various schemas, so let the connector add missing columns
        props.put("auto.evolve", true);
        final DatabendSinkConfig config = new DatabendSinkConfig(props);
        dialect = DatabendClient.create(config);
    }

    @After
    public void teardown() throws Exception {
        DriverManager.setLoginTimeout(defaultLoginTimeout);
    }

    @Test
    public void shouldBuildInsertStatement() throws SQLException {
        TableIdentity tableId = new TableIdentity(null, null, "myTable");
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            colDefs.add(mock(ColumnDefinition.class));
        }
        when(colDefs.get(0).type()).thenReturn(String.valueOf(Types.INTEGER));
        when(colDefs.get(0).id()).thenReturn(new ColumnIdentity(tableId, "id1"));
        when(colDefs.get(0).isPrimaryKey()).thenReturn(true);
        when(colDefs.get(1).type()).thenReturn(String.valueOf(Types.INTEGER));
        when(colDefs.get(1).id()).thenReturn(new ColumnIdentity(tableId, "id2"));
        when(colDefs.get(1).isPrimaryKey()).thenReturn(true);
        when(colDefs.get(2).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(2).id()).thenReturn(new ColumnIdentity(tableId, "columnA"));
        when(colDefs.get(2).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(3).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(3).id()).thenReturn(new ColumnIdentity(tableId, "columnB"));
        when(colDefs.get(3).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(4).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(4).id()).thenReturn(new ColumnIdentity(tableId, "columnC"));
        when(colDefs.get(4).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(5).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(5).id()).thenReturn(new ColumnIdentity(tableId, "columnD"));
        when(colDefs.get(5).isPrimaryKey()).thenReturn(false);

        TableDefinition tableDefn = new TableDefinition(tableId, colDefs);
        assertEquals(
                "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
                        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?)",
                dialect.buildInsertStatement(tableId, pkColumns, columnsAtoD, tableDefn)
        );
    }

    @Test
    public void shouldBuildUpsertStatement() throws SQLException {
        TableIdentity tableId = new TableIdentity(null, null, "myTable");
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            colDefs.add(mock(ColumnDefinition.class));
        }
        when(colDefs.get(0).type()).thenReturn(String.valueOf(Types.INTEGER));
        when(colDefs.get(0).id()).thenReturn(new ColumnIdentity(tableId, "id1"));
        when(colDefs.get(0).isPrimaryKey()).thenReturn(true);
        when(colDefs.get(1).type()).thenReturn(String.valueOf(Types.INTEGER));
        when(colDefs.get(1).id()).thenReturn(new ColumnIdentity(tableId, "id2"));
        when(colDefs.get(1).isPrimaryKey()).thenReturn(true);
        when(colDefs.get(2).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(2).id()).thenReturn(new ColumnIdentity(tableId, "columnA"));
        when(colDefs.get(2).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(3).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(3).id()).thenReturn(new ColumnIdentity(tableId, "columnB"));
        when(colDefs.get(3).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(4).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(4).id()).thenReturn(new ColumnIdentity(tableId, "columnC"));
        when(colDefs.get(4).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(5).type()).thenReturn(String.valueOf(Types.VARCHAR));
        when(colDefs.get(5).id()).thenReturn(new ColumnIdentity(tableId, "columnD"));
        when(colDefs.get(5).isPrimaryKey()).thenReturn(false);

        TableDefinition tableDefn = new TableDefinition(tableId, colDefs);
        assertEquals(
                "REPLACE INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
                        "\"columnC\",\"columnD\") ON (\"id1\",\"id2\") VALUES (?,?,?,?,?,?)",
                dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD, tableDefn)
        );
    }

    @Test
    public void shouldBuildDeleteStatement() throws SQLException {

    }
}
