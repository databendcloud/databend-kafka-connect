package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DatabendHelperTest {
    private final DatabendHelper databendHelper = new DatabendHelper(getClass().getSimpleName());

    @Before
    public void setUp() throws IOException, SQLException {
        databendHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        databendHelper.tearDown();
    }

    @Test
    public void returnTheDatabaseTableInformation() throws SQLException {
        String createEmployees = "CREATE TABLE employees\n" +
                "( employee_id INT, \n" +
                "  last_name VARCHAR NOT NULL,\n" +
                "  first_name VARCHAR,\n" +
                "  hire_date DATE\n" +
                ");";

        String createProducts = "CREATE TABLE products\n" +
                "( product_id INT,\n" +
                "  product_name VARCHAR NOT NULL,\n" +
                "  quantity INTEGER NOT NULL DEFAULT 0\n" +
                ");";

        String createNormalTable = "CREATE TABLE normal (id int, response varchar)";

        databendHelper.createTable(createEmployees);
        databendHelper.createTable(createProducts);
        databendHelper.createTable(createNormalTable);

        Map<String, String> connProps = new HashMap<>();
        connProps.put("connection.user", "databend");
        connProps.put("connection.password", "databend");
        connProps.put("pk.fields","employee_id,product_id");
        connProps.put(DatabendSinkConfig.CONNECTION_URL, databendHelper.databendUri());
        DatabendSinkConfig config = new DatabendSinkConfig(connProps);
        DatabendConnection databendConnection = DatabendClient.create(config);
        ;

        final Map<String, TableDefinition> tables = new HashMap<>();
        for (TableIdentity tableId : databendConnection.tableIds(databendHelper.connection)) {
            tables.put(tableId.tableName(), databendConnection.describeTable(databendHelper.connection, tableId));
        }

        assertTrue(tables.containsKey("employees"));
        assertTrue(tables.containsKey("products"));
        assertTrue(tables.containsKey("normal"));

        TableDefinition normal = tables.get("normal");
        assertEquals(2, normal.columnCount());

        ColumnDefinition colDefn = normal.definitionForColumn("id");
        assertFalse(colDefn.isOptional());
        assertFalse(colDefn.isPrimaryKey());
        assertEquals(DatabendTypes.INT32, colDefn.type());

        colDefn = normal.definitionForColumn("response");
        assertFalse(colDefn.isOptional());
        assertFalse(colDefn.isPrimaryKey());
        assertEquals(DatabendTypes.STRING, colDefn.type());

        TableDefinition employees = tables.get("employees");
        assertEquals(4, employees.columnCount());

        assertNotNull(employees.definitionForColumn("employee_id"));
        assertFalse(employees.definitionForColumn("employee_id").isOptional());
        assertTrue(employees.definitionForColumn("employee_id").isPrimaryKey());
        assertEquals(DatabendTypes.INT32, employees.definitionForColumn("employee_id").type());
        assertNotNull(employees.definitionForColumn("last_name"));
        assertFalse(employees.definitionForColumn("last_name").isOptional());
        assertFalse(employees.definitionForColumn("last_name").isPrimaryKey());
        assertEquals(DatabendTypes.STRING, employees.definitionForColumn("last_name").type());
        assertNotNull(employees.definitionForColumn("first_name"));
        assertFalse(employees.definitionForColumn("first_name").isOptional());
        assertFalse(employees.definitionForColumn("first_name").isPrimaryKey());
        assertEquals(DatabendTypes.STRING, employees.definitionForColumn("first_name").type());
        assertNotNull(employees.definitionForColumn("hire_date"));
        assertFalse(employees.definitionForColumn("hire_date").isOptional());
        assertFalse(employees.definitionForColumn("hire_date").isPrimaryKey());
        // sqlite returns VARCHAR for DATE. why?!
        assertEquals(DatabendTypes.DATE, employees.definitionForColumn("hire_date").type());
        // assertEquals(columns.get("hire_date").getSqlType(), Types.DATE);

        TableDefinition products = tables.get("products");
        assertEquals(4, employees.columnCount());

        assertNotNull(products.definitionForColumn("product_id"));
        assertFalse(products.definitionForColumn("product_id").isOptional());
        assertTrue(products.definitionForColumn("product_id").isPrimaryKey());
        assertEquals(DatabendTypes.INT32, products.definitionForColumn("product_id").type());
        assertNotNull(products.definitionForColumn("product_name"));
        assertFalse(products.definitionForColumn("product_name").isOptional());
        assertFalse(products.definitionForColumn("product_name").isPrimaryKey());
        assertEquals(DatabendTypes.STRING, products.definitionForColumn("product_name").type());
        assertNotNull(products.definitionForColumn("quantity"));
        assertFalse(products.definitionForColumn("quantity").isOptional());
        assertFalse(products.definitionForColumn("quantity").isPrimaryKey());
        assertEquals(DatabendTypes.INT32, products.definitionForColumn("quantity").type());
    }
}
