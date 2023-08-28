package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.databendclient.TableDefinition;
import com.databend.kafka.connect.databendclient.TableIdentity;
import com.databend.kafka.connect.databendclient.TableType;
import com.databend.kafka.connect.sink.metadata.FieldsMetadata;
import com.databend.kafka.connect.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DbStructureTest {

    DatabendConnection dbDialect = mock(DatabendConnection.class);
    DbStructure structure = new DbStructure(dbDialect);
    Connection connection = mock(Connection.class);
    TableIdentity tableId = mock(TableIdentity.class);
    DatabendSinkConfig config = mock(DatabendSinkConfig.class);
    FieldsMetadata fieldsMetadata = new FieldsMetadata(new HashSet<>(), new HashSet<>(), new HashMap<>());

    @Test
    public void testNoMissingFields() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aaa", "bbb")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithSameCase() {
        assertEquals(1, missingFields(sinkRecords("aaa", "bbb"), columns("aaa")).size());
    }

    @Test
    public void testSameNamesDifferentCases() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aAa", "AaA")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithDifferentCase() {
        assertTrue(missingFields(sinkRecords("aaa", "bbb"), columns("AaA", "BbB")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aaa", "bbb")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aAa", "BbB")).isEmpty());
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testMissingTableNoAutoCreate() throws Exception {
        structure.create(config, connection, tableId,
                fieldsMetadata);
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testCreateOrAlterNoAutoEvolve() throws Exception {
        when(dbDialect.tableExists(any(), any())).thenReturn(false);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                false
        );

        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.name()),
                Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

        structure.createOrAmendIfNecessary(config, connection, tableId, fieldsMetadata);
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testAlterNoAutoEvolve() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(any(), any())).thenReturn(true);
        when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.TABLE);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                false
        );

        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.name()),
                Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testAlterNotSupported() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(any(), any())).thenReturn(true);
        when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                true
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.name()),
                Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testCannotAlterBecauseFieldNotOptionalAndNoDefaultValue() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(any(), any())).thenReturn(true);
        when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.INT32_SCHEMA,
                "test",
                true
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.name()),
                Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 5);
    }

    @Test (expected = TableAlterOrCreateException.class)
    public void testFailedToAmendExhaustedRetry() throws Exception {
        TableDefinition tableDefinition = mock(TableDefinition.class);
        when(dbDialect.tableExists(any(), any())).thenReturn(true);
        when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
        when(tableDefinition.type()).thenReturn(TableType.VIEW);

        SinkRecordField sinkRecordField = new SinkRecordField(
                Schema.OPTIONAL_INT32_SCHEMA,
                "test",
                false
        );
        fieldsMetadata = new FieldsMetadata(
                Collections.emptySet(),
                Collections.singleton(sinkRecordField.name()),
                Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

        Map<String, String> props = new HashMap<>();

        // Required configurations, set to empty strings because they are irrelevant for the test
        props.put("connection.url", "");
        props.put("connection.user", "");
        props.put("connection.password", "");

        // Set to true so that the connector does not throw the exception on a different condition
        props.put("auto.evolve", "true");
        DatabendSinkConfig config = new DatabendSinkConfig(props);

        doThrow(new SQLException()).when(dbDialect).applyDdlStatements(any(), any());

        structure.amendIfNecessary(config, connection, tableId,
                fieldsMetadata, 0);
    }

    private Set<SinkRecordField> missingFields(
            Collection<SinkRecordField> fields,
            Set<String> dbColumnNames
    ) {
        return structure.missingFields(fields, dbColumnNames);
    }

    static Set<String> columns(String... names) {
        return new HashSet<>(Arrays.asList(names));
    }

    static List<SinkRecordField> sinkRecords(String... names) {
        List<SinkRecordField> fields = new ArrayList<>();
        for (String n : names) {
            fields.add(field(n));
        }
        return fields;
    }

    static SinkRecordField field(String name) {
        return new SinkRecordField(Schema.STRING_SCHEMA, name, false);
    }
}

