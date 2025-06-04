package stock_jdbc;

import io.crate.testing.CrateTestCluster;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.DriverManager;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class JdbcTest {

    @ClassRule
    public static final CrateTestCluster TEST_CLUSTER = CrateTestCluster
        .fromURL("https://cdn.crate.io/downloads/releases/nightly/crate-latest.tar.gz")
        .settings(Map.of("psql.port", 55433))
        .build();
    public static final String URL = "jdbc:postgresql://localhost:55433/doc?user=crate";

    @Before
    public void before() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var tables = conn.getMetaData().getTables(null, "doc", null, null);
            while (tables.next()) {
                String schema = tables.getString(2);
                String table = tables.getString(3);
                conn.createStatement().execute(
                    "DROP TABLE IF EXISTS \"" + schema + "\".\"" + table + "\"");
            }
        }
    }

    @Test
    public void test_crud_operations_can_be_used() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var stmt = conn.createStatement();
            var hasResultSet = stmt.execute("CREATE TABLE tbl (x int, y int)");
            assertThat(hasResultSet).isFalse();
            assertThat(stmt.getUpdateCount()).isEqualTo(1);

            var insertStmt = conn.prepareStatement("INSERT INTO tbl (x, y) VALUES (?, ?)");
            insertStmt.setInt(1, 1);
            insertStmt.setInt(2, 10);
            insertStmt.execute();

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(results.next()).isTrue();
            assertThat(results.getInt(1)).isEqualTo(1);
            assertThat(results.getInt(2)).isEqualTo(10);
            assertThat(results.next()).isFalse();

            stmt.execute("UPDATE tbl SET y = y + 10");
            stmt.execute("REFRESH TABLE tbl");

            var resultsAfterUpdate = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(resultsAfterUpdate.next()).isTrue();
            assertThat(resultsAfterUpdate.getInt(1)).isEqualTo(1);
            assertThat(resultsAfterUpdate.getInt(2)).isEqualTo(20);
            assertThat(resultsAfterUpdate.next()).isFalse();
        }
    }

    @Test
    public void test_prepare_call_can_be_used_to_insert_records() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var stmt = conn.createStatement();
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var call = conn.prepareCall("INSERT INTO tbl (x, y) VALUES (?, ?)")) {
                call.setInt(1, 1);
                call.setInt(2, 15);
                call.execute();
            }

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT x, y FROM tbl ORDER BY 1");
            assertThat(results.next()).isTrue();
            assertThat(results.getInt(1)).isEqualTo(1);
            assertThat(results.getInt(2)).isEqualTo(15);
            assertThat(results.next()).isFalse();
        }
    }

    @Test
    public void test_batch_insert_is_supported() throws Exception {
        try (var conn = DriverManager.getConnection(URL)) {
            var stmt = conn.createStatement();
            stmt.execute("CREATE TABLE tbl (x int, y int)");

            try (var insert = conn.prepareStatement("INSERT INTO tbl (x, y) VALUES (?, ?)")) {
                for (int i = 0; i < 20; i++) {
                    insert.setInt(1, i);
                    insert.setInt(2, i * 10);
                    insert.addBatch();
                }
                int[] results = insert.executeBatch();
                assertThat(results.length).isEqualTo(20);
            }

            stmt.execute("REFRESH TABLE tbl");

            var results = conn.createStatement().executeQuery("SELECT count(*) FROM tbl");
            assertThat(results.next()).isTrue();
            assertThat(results.getInt(1)).isEqualTo(20);
        }

    }
}
