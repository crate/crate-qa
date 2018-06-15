package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func main() {

	num_batches := 20
	batch_size := 500

	hosts := flag.String("hosts", "", "CrateDB hostname")
	port := flag.Int("port", 5432, "CrateDB postgres port")
	flag.Parse()

	pgxConfig := pgx.ConnConfig{
		Host:     *hosts,
		Port:     uint16(*port),
		Database: "doc",
		User:     "crate",
	}

	conn, err := pgx.Connect(pgxConfig)
	if err != nil {
		log.Fatal(err)
	}

	_, err = conn.Prepare("ps1", "INSERT INTO users (id, name, value) VALUES ($1, $2, $3)")
	if err != nil {
		log.Fatal(err)
	}

	for b := 0; b < num_batches; b++ {
		batch := conn.BeginBatch()
		fmt.Println("batch " + strconv.Itoa(b))

		for x := 0; x < batch_size; x++ {
			id := b*x + x
			username := "user_" + strconv.Itoa(b) + "_" + strconv.Itoa(x)
			batch.Queue("ps1",
				[]interface{}{id, username, rand.Float32()},
				[]pgtype.OID{pgtype.Int4OID, pgtype.VarcharOID, pgtype.Float4OID},
				nil,
			)
		}

		err = batch.Send(context.Background(), nil)
		if err != nil {
			log.Fatal(err)
		}

		err = batch.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

}
