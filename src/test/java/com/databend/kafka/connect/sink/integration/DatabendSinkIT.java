package com.databend.kafka.connect.sink.integration;

import com.databend.kafka.connect.sink.DatabendSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class DatabendSinkIT extends BaseConnectorIT {

    Connection connection;
    private JsonConverter jsonConverter;
    private Map<String, String> props;

    private static final String KEY = "key";
    private static final String MEASUREMENTS = "measurements";

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        startConnect();

        jsonConverter = jsonConverter();

        props = baseSinkProps();
        props.put(DatabendSinkConfig.CONNECTION_URL, "jdbc:databend://localhost:8000");
        props.put(DatabendSinkConfig.CONNECTION_USER, "databend");
        props.put(DatabendSinkConfig.CONNECTION_PASSWORD, "databend");
        props.put(DatabendSinkConfig.PK_MODE, "record_value");
        props.put(DatabendSinkConfig.PK_FIELDS, KEY);
        props.put(DatabendSinkConfig.AUTO_CREATE, "false");
        props.put(DatabendSinkConfig.INSERT_MODE, "insert");
        props.put(DatabendSinkConfig.TABLE_TYPES_CONFIG, "TABLE");
        props.put("topics", MEASUREMENTS);

        // create topic in Kafka
        connect.kafka().createTopic(MEASUREMENTS, 1);
        Class.forName("com.databend.jdbc.DatabendDriver");

        createTable();
    }

    private void createTable() throws SQLException {
        connection = DriverManager.getConnection(
                "jdbc:databend://localhost:8000",
                "databend",
                "databend");

        try (Statement s = connection.createStatement()) {
            s.execute("DROP TABLE measurements;");
        }

        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + MEASUREMENTS + " ("
                    + "    key             int not null,"
                    + "    city_id         int not null,"
                    + "    logdate         date not null,"
                    + "    peaktemp        int,"
                    + "    unitsales       int"
                    + ");");
        }
    }


    @After
    public void tearDown() throws SQLException {
//        connection.close();
        stopConnect();
    }

    @Test
    public void shouldInsertMeasurement() throws Exception {
        Schema schema = SchemaBuilder.struct()
                .field(KEY, Schema.INT32_SCHEMA)
                .field("city_id", Schema.INT32_SCHEMA)
                .field("logdate", org.apache.kafka.connect.data.Date.SCHEMA)
                .field("peaktemp", Schema.INT32_SCHEMA)
                .field("unitsales", Schema.INT32_SCHEMA)
                .build();

        java.util.Date logdate = new java.util.Date(0);

        org.apache.kafka.connect.data.Struct value = new org.apache.kafka.connect.data.Struct(schema)
                .put(KEY, 1)
                .put("city_id", 2)
                .put("logdate", logdate)
                .put("peaktemp", 4)
                .put("unitsales", 5);

        String connectorName = "databend-sink-connector";
        connect.configureConnector(connectorName, props);
        waitForConnectorToStart(connectorName, 1);

        produceRecord(schema, value);

        waitForCommittedRecords(connectorName, Collections.singleton(MEASUREMENTS), 1, 1, TimeUnit.MINUTES.toMillis(3));

        assertSinkRecordValues(value);
    }

    private void assertSinkRecordValues(org.apache.kafka.connect.data.Struct value) throws SQLException, Exception {
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM " + MEASUREMENTS
                     + " ORDER BY KEY LIMIT 1")) {
            assertTrue(rs.next());

            assertEquals(value.getInt32(KEY).intValue(), rs.getInt(1));
            assertEquals(value.getInt32("city_id").intValue(), rs.getInt(2));
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            assertEquals(value.get("logdate"), rs.getDate(3, calendar));
            assertEquals(value.getInt32("peaktemp").intValue(), rs.getInt(4));
            assertEquals(value.getInt32("unitsales").intValue(), rs.getInt(5));
        }
    }

    private void produceRecord(Schema schema, org.apache.kafka.connect.data.Struct struct) {
        String kafkaValue = new String(jsonConverter.fromConnectData(MEASUREMENTS, schema, struct));
        connect.kafka().produce(MEASUREMENTS, null, kafkaValue);
    }
}
