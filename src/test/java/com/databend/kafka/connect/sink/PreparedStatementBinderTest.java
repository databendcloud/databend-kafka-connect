package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.*;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SchemaPair;
import com.databend.kafka.connect.util.DateTimeUtils;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.time.ZoneOffset;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class PreparedStatementBinderTest {

    private DatabendConnection dialect;

    @Before
    public void beforeEach() {
        Map<String, String> props = new HashMap<>();
        props.put(DatabendSinkConfig.CONNECTION_URL, "jdbc:databend://localhost:8000");
        props.put(DatabendSinkConfig.CONNECTION_USER, "databend");
        props.put(DatabendSinkConfig.CONNECTION_PASSWORD, "databend");
        DatabendSinkConfig config = new DatabendSinkConfig(props);
        dialect = DatabendClient.create(config);
    }

    @Test
    public void bindRecordInsert() throws SQLException, ParseException {
        Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("bool", Schema.BOOLEAN_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("decimal", Decimal.schema(0))
                .field("date", Date.SCHEMA)
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("firstname", "Cloud")
                .put("lastname", "Han")
                .put("bool", true)
                .put("short", (short) 1234)
                .put("byte", (byte) -32)
                .put("long", (long) 12425436)
                .put("float", (float) 2356.3)
                .put("double", -2436546.56457)
                .put("bytes", new byte[]{-32, 124})
                .put("age", 30)
                .put("decimal", new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN))
                .put("date", new java.util.Date(0))
                .put("time", new java.util.Date(1000))
                .put("timestamp", new java.util.Date(100));

        SchemaPair schemaPair = new SchemaPair(null, valueSchema);

        DatabendSinkConfig.PrimaryKeyMode pkMode = DatabendSinkConfig.PrimaryKeyMode.RECORD_VALUE;

        List<String> pkFields = Collections.singletonList("long");

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract("people", pkMode, pkFields, Collections.<String>emptySet(), schemaPair);

        PreparedStatement statement = mock(PreparedStatement.class);
        TableIdentity tabId = new TableIdentity("ORCL", "ADMIN", "people");
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (int i = 0; i < 14; i++) {
            colDefs.add(mock(ColumnDefinition.class));
        }
        when(colDefs.get(0).type()).thenReturn(String.valueOf(Types.NVARCHAR));
        when(colDefs.get(0).id()).thenReturn(new ColumnIdentity(tabId, "firstname"));
        when(colDefs.get(0).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(1).type()).thenReturn(String.valueOf(Types.NVARCHAR));
        when(colDefs.get(1).id()).thenReturn(new ColumnIdentity(tabId, "lastname"));
        when(colDefs.get(1).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(2).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(2).id()).thenReturn(new ColumnIdentity(tabId, "age"));
        when(colDefs.get(2).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(3).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(3).id()).thenReturn(new ColumnIdentity(tabId, "bool"));
        when(colDefs.get(3).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(4).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(4).id()).thenReturn(new ColumnIdentity(tabId, "short"));
        when(colDefs.get(4).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(5).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(5).id()).thenReturn(new ColumnIdentity(tabId, "byte"));
        when(colDefs.get(5).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(6).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(6).id()).thenReturn(new ColumnIdentity(tabId, "long"));
        when(colDefs.get(6).isPrimaryKey()).thenReturn(true);
        // BINARY_FLOAT = 100
        when(colDefs.get(7).type()).thenReturn(String.valueOf(100));
        when(colDefs.get(7).id()).thenReturn(new ColumnIdentity(tabId, "float"));
        when(colDefs.get(7).isPrimaryKey()).thenReturn(false);
        // BINARY_DOUBLE = 101
        when(colDefs.get(8).type()).thenReturn(String.valueOf(101));
        when(colDefs.get(8).id()).thenReturn(new ColumnIdentity(tabId, "double"));
        when(colDefs.get(8).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(9).type()).thenReturn(String.valueOf(Types.BLOB));
        when(colDefs.get(9).id()).thenReturn(new ColumnIdentity(tabId, "bytes"));
        when(colDefs.get(9).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(10).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(10).id()).thenReturn(new ColumnIdentity(tabId, "decimal"));
        when(colDefs.get(10).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(11).type()).thenReturn(String.valueOf(Types.DATE));
        when(colDefs.get(11).id()).thenReturn(new ColumnIdentity(tabId, "date"));
        when(colDefs.get(11).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(12).type()).thenReturn(String.valueOf(Types.DATE));
        when(colDefs.get(12).id()).thenReturn(new ColumnIdentity(tabId, "time"));
        when(colDefs.get(12).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(13).type()).thenReturn(String.valueOf(Types.TIMESTAMP));
        when(colDefs.get(13).id()).thenReturn(new ColumnIdentity(tabId, "timestamp"));
        when(colDefs.get(13).isPrimaryKey()).thenReturn(false);
        TableDefinition tabDef = new TableDefinition(tabId, colDefs);

        PreparedStatementBinder binder = new PreparedStatementBinder(
                dialect,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata,
                tabDef,
                DatabendSinkConfig.InsertMode.INSERT
        );

        binder.bindRecord(new SinkRecord("topic", 0, null, null, valueSchema, valueStruct, 0));

        int index = 1;
        // key field first
        verify(statement, times(1)).setLong(index++, valueStruct.getInt64("long"));
        // rest in order of schema def
        verify(statement, times(1)).setString(index++, valueStruct.getString("firstname"));
        verify(statement, times(1)).setString(index++, valueStruct.getString("lastname"));
        verify(statement, times(1)).setInt(index++, valueStruct.getInt32("age"));
        verify(statement, times(1)).setBoolean(index++, valueStruct.getBoolean("bool"));
        verify(statement, times(1)).setShort(index++, valueStruct.getInt16("short"));
        verify(statement, times(1)).setByte(index++, valueStruct.getInt8("byte"));
        verify(statement, times(1)).setFloat(index++, valueStruct.getFloat32("float"));
        verify(statement, times(1)).setDouble(index++, valueStruct.getFloat64("double"));
        verify(statement, times(1)).setBytes(index++, valueStruct.getBytes("bytes"));
        verify(statement, times(1)).setBigDecimal(index++, (BigDecimal) valueStruct.get("decimal"));
    }

    @Test
    public void bindRecordUpsertMode() throws SQLException, ParseException {
        Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("firstname", "Alex")
                .put("long", (long) 12425436);

        SchemaPair schemaPair = new SchemaPair(null, valueSchema);

        DatabendSinkConfig.PrimaryKeyMode pkMode = DatabendSinkConfig.PrimaryKeyMode.RECORD_VALUE;

        List<String> pkFields = Collections.singletonList("long");

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract("people", pkMode, pkFields, Collections.<String>emptySet(), schemaPair);

        PreparedStatement statement = mock(PreparedStatement.class);
        TableIdentity tabId = new TableIdentity("ORCL", "ADMIN", "people");
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            colDefs.add(mock(ColumnDefinition.class));
        }
        when(colDefs.get(0).type()).thenReturn(String.valueOf(Types.NVARCHAR));
        when(colDefs.get(0).id()).thenReturn(new ColumnIdentity(tabId, "firstname"));
        when(colDefs.get(0).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(1).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(1).id()).thenReturn(new ColumnIdentity(tabId, "long"));
        when(colDefs.get(1).isPrimaryKey()).thenReturn(true);
        TableDefinition tabDef = new TableDefinition(tabId, colDefs);

        PreparedStatementBinder binder = new PreparedStatementBinder(
                dialect,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata, tabDef, DatabendSinkConfig.InsertMode.UPSERT
        );

        binder.bindRecord(new SinkRecord("topic", 0, null, null, valueSchema, valueStruct, 0));

        int index = 1;
        // key field first
        verify(statement, times(1)).setLong(index++, valueStruct.getInt64("long"));
        // rest in order of schema def
        verify(statement, times(1)).setString(index++, valueStruct.getString("firstname"));
    }

    @Test
    public void bindRecordUpdateMode() throws SQLException, ParseException {
        Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("firstname", "Alex")
                .put("long", (long) 12425436);

        SchemaPair schemaPair = new SchemaPair(null, valueSchema);

        DatabendSinkConfig.PrimaryKeyMode pkMode = DatabendSinkConfig.PrimaryKeyMode.RECORD_VALUE;

        List<String> pkFields = Collections.singletonList("long");

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract("people", pkMode, pkFields,
                Collections.<String>emptySet(), schemaPair);

        PreparedStatement statement = mock(PreparedStatement.class);
        TableIdentity tabId = new TableIdentity("ORCL", "ADMIN", "people");
        List<ColumnDefinition> colDefs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            colDefs.add(mock(ColumnDefinition.class));
        }
        when(colDefs.get(0).type()).thenReturn(String.valueOf(Types.NVARCHAR));
        when(colDefs.get(0).id()).thenReturn(new ColumnIdentity(tabId, "firstname"));
        when(colDefs.get(0).isPrimaryKey()).thenReturn(false);
        when(colDefs.get(1).type()).thenReturn(String.valueOf(Types.NUMERIC));
        when(colDefs.get(1).id()).thenReturn(new ColumnIdentity(tabId, "long"));
        when(colDefs.get(1).isPrimaryKey()).thenReturn(true);
        TableDefinition tabDef = new TableDefinition(tabId, colDefs);

        PreparedStatementBinder binder = new PreparedStatementBinder(
                dialect,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata, tabDef, DatabendSinkConfig.InsertMode.UPDATE
        );

        binder.bindRecord(new SinkRecord("topic", 0, null, null, valueSchema, valueStruct, 0));

        int index = 1;

        // non key first
        verify(statement, times(1)).setString(index++, valueStruct.getString("firstname"));
        // last the keys
        verify(statement, times(1)).setLong(index++, valueStruct.getInt64("long"));
    }

}

