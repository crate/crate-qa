package main

import (
	"flag"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"time"
)

func main() {
	hosts := flag.String("hosts", "", "CrateDB hostname")
	port := flag.Int("port", 5432, "CrateDB postgres port")
	flag.Parse()

	pgxConfig := pgx.ConnConfig{
		Host:     *hosts,
		Port:     uint16(*port),
		Database: "doc",
		User:     "crate"}
	conn, err := pgx.Connect(pgxConfig)
	if err != nil {
		log.Fatal(err)
	}
	var name string
	err = conn.QueryRow("select name || $1 from sys.cluster", "foo").Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
	commandTag, err := conn.Exec("create table t1 (x integer, ts timestamp)")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
	ts := time.Now()
	commandTag, err = conn.Exec("insert into t1 (x, ts) values (?, ?)", 1, ts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("refresh table t1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
	var tsRead time.Time
	err = conn.QueryRow("select ts from t1").Scan(&tsRead)
	if err != nil {
		log.Fatal(err)
	}
	if (tsRead.Sub(ts) > (1 * time.Second)) {
		log.Fatal("Inserted ts doesn't match read ts: ", ts, tsRead)
	}
	commandTag, err = conn.Exec("update t1 set x = ?", 2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("refresh table t1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("delete from t1 where x = ?", 2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(commandTag)
}
