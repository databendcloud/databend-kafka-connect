package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.util.DateTimeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;

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

    @Test
    public void retries() throws SQLException {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        List<SinkRecord> records = createRecordsList(1);

        mockWriter.write(records);
        SQLException chainedException = new SQLException("cause 1");
        chainedException.setNextException(new SQLException("cause 2"));
        chainedException.setNextException(new SQLException("cause 3"));
        expectLastCall().andThrow(chainedException).times(1 + maxRetries);

        ctx.timeout(retryBackoffMs);
        expectLastCall().times(maxRetries);

        mockWriter.closeQuietly();
        expectLastCall().times(maxRetries);

        DatabendSinkTask task = new DatabendSinkTask() {
            @Override
            void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);
        expect(ctx.errantRecordReporter()).andReturn(null);
        replayAll();

        Map<String, String> props = setupBasicProps(maxRetries, retryBackoffMs);
        task.start(props);

        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
            assertEquals(SQLException.class, expected.getCause().getClass());
            int i = 0;
            for (Throwable t : (SQLException) expected.getCause()) {
                ++i;
                StringWriter sw = new StringWriter();
                t.printStackTrace(new PrintWriter(sw));
                System.out.println("Chained exception " + i + ": " + sw);
            }
        }

        try {
            task.put(records);
            fail();
        } catch (RetriableException expected) {
            assertEquals(SQLException.class, expected.getCause().getClass());
            int i = 0;
            for (Throwable t : (SQLException) expected.getCause()) {
                ++i;
                StringWriter sw = new StringWriter();
                t.printStackTrace(new PrintWriter(sw));
                System.out.println("Chained exception " + i + ": " + sw);
            }
        }

        try {
            task.put(records);
            fail();
        } catch (RetriableException e) {
            fail("Non-retriable exception expected");
        } catch (ConnectException expected) {
            assertEquals(SQLException.class, expected.getCause().getClass());
            int i = 0;
            for (Throwable t : (SQLException) expected.getCause()) {
                ++i;
                StringWriter sw = new StringWriter();
                t.printStackTrace(new PrintWriter(sw));
                System.out.println("Chained exception " + i + ": " + sw);
            }
        }

        verifyAll();
    }

    @Test
    public void batchErrorReporting() throws SQLException {
        final int batchSize = 3;

        List<SinkRecord> records = createRecordsList(batchSize);

        mockWriter.write(records);
        SQLException exception = new SQLException("cause 1");
        expectLastCall().andThrow(exception);
        mockWriter.closeQuietly();
        expectLastCall();
        mockWriter.write(anyObject());
        expectLastCall().andThrow(exception).times(batchSize);

        DatabendSinkTask task = new DatabendSinkTask() {
            @Override
            void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
        expect(ctx.errantRecordReporter()).andReturn(reporter);
        expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null)).times(batchSize);
        for (int i = 0; i < batchSize; i++) {
            mockWriter.closeQuietly();
            expectLastCall();
        }
        replayAll();

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
        verifyAll();
    }

    @Test
    public void errorReportingTableAlterOrCreateException() throws SQLException {
        List<SinkRecord> records = createRecordsList(1);

        mockWriter.write(records);
        TableAlterOrCreateException exception = new TableAlterOrCreateException("cause 1");
        expectLastCall().andThrow(exception);
        mockWriter.closeQuietly();
        expectLastCall();
        mockWriter.write(anyObject());
        expectLastCall().andThrow(exception);

        DatabendSinkTask task = new DatabendSinkTask() {
            @Override
            void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
        expect(ctx.errantRecordReporter()).andReturn(reporter);
        expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
        mockWriter.closeQuietly();
        expectLastCall();
        replayAll();

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
        verifyAll();
    }

    @Test
    public void oneInBatchErrorReporting() throws SQLException {
        final int batchSize = 3;

        List<SinkRecord> records = createRecordsList(batchSize);

        mockWriter.write(records);
        SQLException exception = new SQLException("cause 1");
        expectLastCall().andThrow(exception);
        mockWriter.closeQuietly();
        expectLastCall();
        mockWriter.write(anyObject());
        expectLastCall().times(2);
        expectLastCall().andThrow(exception);

        DatabendSinkTask task = new DatabendSinkTask() {
            @Override
            void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);
        ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
        expect(ctx.errantRecordReporter()).andReturn(reporter);
        expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
        mockWriter.closeQuietly();
        expectLastCall();
        replayAll();

        Map<String, String> props = setupBasicProps(0, 0);
        task.start(props);
        task.put(records);
        verifyAll();
    }

    private List<SinkRecord> createRecordsList(int batchSize) {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            records.add(RECORD);
        }
        return records;
    }

    private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
        Map<String, String> props = new HashMap<>();
        props.put(DatabendSinkConfig.CONNECTION_URL, "jdbc:databend://databend:databend@localhost:8000/default");
        props.put(DatabendSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(DatabendSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
        return props;
    }
}
