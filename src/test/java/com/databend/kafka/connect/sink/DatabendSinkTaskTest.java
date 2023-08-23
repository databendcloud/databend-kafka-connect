package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.util.DateTimeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatabendSinkTaskTest extends EasyMockSupport {
    private final DatabendHelper databendHelper = new DatabendHelper(getClass().getSimpleName());
    private final DatabendWriter mockWriter = createMock(DatabendWriter.class);
    private final SinkTaskContext ctx = createMock(SinkTaskContext.class);

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstname", Schema.STRING_SCHEMA)
            .field("lastname", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .build();

    private static final SinkRecord RECORD = new SinkRecord(
            "stub",
            0,
            null,
            null,
            null,
            null,
            0
    );

    @Before
    public void setUp() throws IOException, SQLException {
        databendHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        databendHelper.tearDown();
    }

    @Test
    public void putPropagatesToDbWithPkModeRecordValue() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", databendHelper.databendUri());
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "firstname,lastname");

        DatabendSinkTask task = new DatabendSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        final String topic = "atopic";

        databendHelper.createTable(
                "CREATE TABLE " + topic + "(" +
                        "    firstname  String," +
                        "    lastname  String," +
                        "    age Int," +
                        "    bool  Boolean," +
                        "    byte  Int," +
                        "    short Int NULL," +
                        "    long Int," +
                        "    float float," +
                        "    double double," +
                        "    modified DATETIME);"
        );

        task.start(props);

        final Struct struct = new Struct(SCHEMA)
                .put("firstname", "Christina")
                .put("lastname", "Brams")
                .put("bool", false)
                .put("byte", (byte) -72)
                .put("long", 8594L)
                .put("double", 3256677.56457d)
                .put("age", 28)
                .put("modified", new Date(1474661402123L));

        task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, SCHEMA, struct, 43)));

        assertEquals(
                1,
                databendHelper.select(
                        "SELECT * FROM " + topic + " WHERE firstname='" + struct.getString("firstname") + "' and lastname='" + struct.getString("lastname") + "'",
                        new DatabendHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                                rs.getShort("short");
                                assertTrue(rs.wasNull());
                                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                                rs.getShort("float");
                                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                                java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                        "modified",
                                        DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.systemDefault()))
                                );
                                assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                            }
                        }
                )
        );
    }
}
