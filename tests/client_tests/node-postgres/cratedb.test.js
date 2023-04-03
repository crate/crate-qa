const pg = require("pg");
const Cursor = require("pg-cursor");

let conn;
let pool;

beforeEach(async () => {
  pool = new pg.Pool({
    user: "crate",
    password: "",
    host: "127.0.0.1",
    port: 5432
  })
  conn = await pool.connect();
  return conn;
});

afterEach(async () => {
  conn.release();
});



describe("queries on table", () => {
  beforeEach(async () => {
    await pool.query(`
      CREATE TABLE tbl (
        log_time TIMESTAMP,
        client_ip IP,
        request TEXT,
        status_code SHORT,
        object_size BIGINT
      )`
    );
  });

  afterEach(async () => {
    await pool.query("DROP TABLE IF EXISTS tbl");
  });

  test("Can insert timestamp via parameter", async () => {
    await pool.query(
      "insert into tbl (log_time) values ($1)",
      ['2021-01-13T14:37:17.25988Z']
    );
    await pool.query("refresh table tbl");
    const resp = await pool.query("select * from tbl");
    expect(resp.rows).toHaveLength(1);
    expect(resp.rows[0]["log_time"]).toStrictEqual(new Date('2021-01-13T14:37:17.25988Z'));
  });

  test("Can insert multiple values", async () => {
    await pool.query(
      `insert into tbl (log_time, client_ip, request, status_code, object_size)
        values
          ('2012-01-01T00:00:00Z', '25.152.171.147',  '/books/Six_Easy_Pieces.html', 404, 271),
          ('2012-01-01T00:00:03Z', '243.180.100.114',  '/slideshow/1.jpg', 304, 0),
          ('2012-01-01T00:00:03Z', '149.60.38.76', '/courses/cs100/finalprojects/adventure/javadocs_/index.html?index-filesindex-16.html', 200, 705),
          ('2012-01-01T00:00:10Z', '243.180.100.114', '/slideshow/2.jpg', 304, 0),
          ('2012-01-01T00:00:11Z', '134.121.15.97', '/courses/cs101/old/2002/syllabus.html', 404, 277),
          ('2012-01-01T00:00:17Z', '243.180.100.114', '/slideshow/3.jpg', 304, 0),
          ('2012-01-01T00:00:17Z', '252.202.20.160', '/degrees/masters/', 200, 3233),
          ('2012-01-01T00:00:17Z', '252.202.20.160', '/degrees/masters/masters.gif', 200, 7921),
          ('2012-01-01T00:00:18Z', '149.60.38.76', '/people/wvv/marron/', 200, 1642),
          ('2012-01-01T00:00:18Z', '134.121.15.97', '/about/rooms/345/', 304, 0)
        `
    );
    await pool.query("refresh table tbl");
    let resp = await pool.query("select * from tbl");
    expect(resp.rows).toHaveLength(10);
  });

  test("Can use update returning", async () => {
    await pool.query(
      `insert into tbl (log_time, client_ip, request, status_code, object_size)
        values
          ('2012-01-01T00:00:00Z', '25.152.171.147',  '/books/One.html', 404, 271),
          ('2012-01-01T00:00:00Z', '1.1.1.1',  '/books/Two.html', 404, 271)
      `
    );
    await pool.query("refresh table tbl");
    resp = await pool.query("update tbl set object_size = 40 where object_size = 271 returning *");
    expect(resp.rows).toHaveLength(2);
    expect(resp.rows[0]["object_size"]).toBe("40");
    expect(resp.rows[1]["object_size"]).toBe("40");
  });
});
