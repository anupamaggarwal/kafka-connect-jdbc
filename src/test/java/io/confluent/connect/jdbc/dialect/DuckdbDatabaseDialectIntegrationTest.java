package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.junit.Before;
import org.junit.Test;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DuckdbDatabaseDialectIntegrationTest extends BaseDialectTest<DuckdbDatabaseDialect> {

    private static String jdbcUrl;

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    protected DuckdbDatabaseDialect createDialect() {
        File tempDb = null;
        try {
            tempDb = File.createTempFile("test-duckdb", ".db");
            String dbPath = tempDb.getAbsolutePath();
            tempDb.delete();
            jdbcUrl = "jdbc:duckdb:" + dbPath;
            DuckdbDatabaseDialect databaseDialect = new DuckdbDatabaseDialect(sourceConfigWithUrl(jdbcUrl));
            Statement stmt = databaseDialect.getConnection().createStatement();
                stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR, active BOOLEAN)");
                stmt.execute("INSERT INTO test_table (id, name, active) VALUES (1, 'Alice', true), (2, 'Bob', false)");

            return databaseDialect;

        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Test
    public void testTableIds() throws Exception {
        Connection connection = dialect.getConnection();
        List<TableId> tables = dialect.tableIds(connection);
        assertTrue(tables.stream().anyMatch(t -> t.tableName().equalsIgnoreCase("test_table")));
    }

    @Test
    public void testDescribeColumns() throws Exception {
        Connection connection = dialect.getConnection();

        Map<ColumnId, io.confluent.connect.jdbc.util.ColumnDefinition> columns =
                dialect.describeColumns(connection, "test_table", null);
        assertTrue(columns.keySet().stream().anyMatch(c -> c.name().equalsIgnoreCase("id")));
        assertTrue(columns.keySet().stream().anyMatch(c -> c.name().equalsIgnoreCase("name")));
        assertTrue(columns.keySet().stream().anyMatch(c -> c.name().equalsIgnoreCase("active")));
    }

    @Test
    public void testTableExists() throws Exception {
        Connection connection = dialect.getConnection();

        TableId tableId = new TableId(null, null, "test_table");
        assertTrue(dialect.tableExists(connection, tableId));
        TableId missing = new TableId(null, null, "not_a_table");
        assertFalse(dialect.tableExists(connection, missing));
    }

    @Test
    public void testCurrentTimeOnDB() throws Exception {
        Connection connection = dialect.getConnection();

        Timestamp ts = dialect.currentTimeOnDB(connection, Calendar.getInstance());
        assertNotNull(ts);
    }

    @Test
    public void testInsertAndQuery() throws Exception {
        Connection connection = dialect.getConnection();

        // Insert a row using the dialect's prepared statement
        String insertSql = "INSERT INTO test_table (id, name, active) VALUES (?, ?, ?)";
        try (PreparedStatement ps = dialect.createPreparedStatement(connection, insertSql)) {
            ps.setInt(1, 3);
            ps.setString(2, "Charlie");
            ps.setBoolean(3, true);
            assertEquals(1, ps.executeUpdate());
        }

        // Query and check
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM test_table WHERE id = 3")) {
            assertTrue(rs.next());
            assertEquals("Charlie", rs.getString(1));
        }
    }
}