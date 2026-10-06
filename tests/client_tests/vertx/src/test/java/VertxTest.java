import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public class VertxTest {

    @Test
    public void test_pipelined_insert_and_select() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            PgConnectOptions options = new PgConnectOptions()
                .setHost("localhost")
                .setPort(5432)
                .setUser("crate")
                .setDatabase("doc");
            PgConnection conn = PgConnection.connect(vertx, options).await();
            conn.query("CREATE TABLE IF NOT EXISTS o (id INT)").execute().await();

            Future<RowSet<Row>> insert = conn.preparedQuery("INSERT INTO o (id) VALUES ($1)").execute(Tuple.of(1));
            Future<RowSet<Row>> select = conn.preparedQuery("SELECT 1").execute();

            assertThat(insert.await(500, TimeUnit.SECONDS).rowCount()).isEqualTo(1);

            assertThat(select.await(500, TimeUnit.SECONDS).iterator().next().getInteger(0)).isEqualTo(1);
        } finally {
            vertx.close().await();
        }
    }
}
