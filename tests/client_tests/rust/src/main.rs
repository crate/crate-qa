use postgres::{Client, NoTls};


fn main() {
    let mut client = Client::connect("host=localhost user=crate", NoTls).unwrap();
    client
        .simple_query("CREATE TABLE tbl (id int primary key, x int not null, name text not null);")
        .unwrap();

    client
        .execute(
            "INSERT INTO tbl (id, x, name) VALUES (1, 10, 'Arthur')",
            &[],
        )
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO tbl (id, x, name) values ($1, $2, $3)")
        .unwrap();

    let id = 2;
    let x = 20;
    let name = "Trillian";
    client.execute(&stmt, &[&id, &x, &name]).unwrap();
    client.execute("refresh table tbl", &[]).unwrap();

    for row in client.query("SELECT id, x, name FROM tbl", &[]).unwrap() {
        let id: i32 = row.get("id");
        let x: i32 = row.get("x");
        let name: &str = row.get("name");
        println!("id={} x={} name={}", &id, &x, &name);
        assert!(id == 1 || id == 2, "id = {} but should be 1 or 2", id);
    }
}
