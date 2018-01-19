package main

import (
	"flag"
	"fmt"
	"github.com/jackc/pgx"
	"log"
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
		return
	}
	var name string
	err = conn.QueryRow("select name || $1 from sys.cluster", "foo").Scan(&name)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(name)
	commandTag, err := conn.Exec("create table t1 (x int)")
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("insert into t1 (x) values (1)")
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("refresh table t1")
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("update t1 set x = ?", 2)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("refresh table t1")
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
	commandTag, err = conn.Exec("delete from t1 where x = ?", 2)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println(commandTag)
}