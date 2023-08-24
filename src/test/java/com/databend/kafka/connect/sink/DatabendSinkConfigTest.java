package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.TableType;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class DatabendSinkConfigTest {
    private Map<String, String> props = new HashMap<>();
    private DatabendSinkConfig config;

    @Before
    public void beforeEach() {
        props.put("connection.url", "jdbc:databend://something"); // just test
    }

    @After
    public void afterEach() {
        props.clear();
        config = null;
    }

    @Test(expected = ConfigException.class)
    public void shouldFailToCreateConfigWithoutConnectionUrl() {
        props.remove(DatabendSinkConfig.CONNECTION_URL);
        createConfig();
    }

    @Test(expected = ConfigException.class)
    public void shouldFailToCreateConfigWithEmptyTableNameFormat() {
        props.put(DatabendSinkConfig.TABLE_NAME_FORMAT, "");
        createConfig();
    }

    @Test
    public void shouldCreateConfigWithMinimalConfigs() {
        createConfig();
        assertTableTypes(TableType.TABLE);
    }

    @Test
    public void shouldCreateConfigWithAdditionalConfigs() {
        props.put("auto.create", "true");
        props.put("pk.fields", "id");
        createConfig();
        assertTableTypes(TableType.TABLE);
    }

    @Test
    public void shouldCreateConfigWithViewOnly() {
        props.put("table.types", "view");
        createConfig();
        assertTableTypes(TableType.VIEW);
    }

    @Test
    public void shouldCreateConfigWithTableOnly() {
        props.put("table.types", "table");
        createConfig();
        assertTableTypes(TableType.TABLE);
    }


    @Test
    public void shouldCreateConfigWithViewAndTable() {
        props.put("table.types", "view,table");
        createConfig();
        assertTableTypes(TableType.TABLE, TableType.VIEW);
        props.put("table.types", "table,view");
        createConfig();
        assertTableTypes(TableType.TABLE, TableType.VIEW);
        props.put("table.types", "table , view");
        createConfig();
        assertTableTypes(TableType.TABLE, TableType.VIEW);
    }

    @Test
    public void shouldCreateConfigWithLeadingWhitespaceInTableTypes() {
        props.put("table.types", " \t\n  view");
        createConfig();
        assertTableTypes(TableType.VIEW);
    }

    @Test
    public void shouldCreateConfigWithTrailingWhitespaceInTableTypes() {
        props.put("table.types", "table \t \n");
        createConfig();
        assertTableTypes(TableType.TABLE);
    }

    protected void createConfig() {
        config = new DatabendSinkConfig(props);
    }

    protected void assertTableTypes(TableType... types) {
        EnumSet<TableType> expected = EnumSet.copyOf(Arrays.asList(types));
        EnumSet<TableType> tableTypes = config.tableTypes;
        assertEquals(expected, tableTypes);
    }
}
