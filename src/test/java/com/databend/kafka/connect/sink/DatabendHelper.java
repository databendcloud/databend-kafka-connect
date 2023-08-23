package com.databend.kafka.connect.sink;

import java.io.IOException;
import java.sql.*;

public class DatabendHelper {
    static {
        try {
            Class.forName("com.databend.jdbc.DatabendDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }

    public final String tableName;
    public final String createSQL;

    public Connection connection;

    public DatabendHelper(String simpleName) {
        tableName = "databend_kafka";
        createSQL = "create table if not exists databend_kafka(a int, b varchar,c int)";
    }

    public String databendUri() {
        return "jdbc:databend://databend:databend@localhost:8000/default";
    }

    public void setUp() throws SQLException, IOException {
        connection = DriverManager.getConnection(databendUri());
        connection.setAutoCommit(false);
        createTable(createSQL);
    }

    public void tearDown() throws SQLException, IOException {
        connection.close();
        deleteTable(tableName);
        deleteTable("employees");
        deleteTable("products");
        deleteTable("normal");
    }

    public void createTable(final String createSql) throws SQLException {
        execute(createSql);
    }

    public void deleteTable(final String table) throws SQLException {
        execute("DROP TABLE IF EXISTS " + table);

        //random errors of table not being available happens in the unit tests
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int select(final String query, final DatabendHelper.ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    public void execute(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            connection.commit();
        }
    }

}
