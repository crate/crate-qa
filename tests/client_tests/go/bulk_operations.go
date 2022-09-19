package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"

	"github.com/jackc/pgx/v5"
)

func main() {

	num_batches := 20
	batch_size := 500

	hosts := flag.String("hosts", "", "CrateDB hostname")
	port := flag.Int("port", 5432, "CrateDB postgres port")
	flag.Parse()

	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://crate@%s:%d/doc", *hosts, *port)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = conn.Prepare(ctx, "ps1", "INSERT INTO users (id, name, value) VALUES ($1, $2, $3)")
	if err != nil {
		log.Fatal(err)
	}

	for b := 0; b < num_batches; b++ {
		fmt.Println("batch " + strconv.Itoa(b))

		batch := &pgx.Batch{}
		for x := 0; x < batch_size; x++ {
			id := b*x + x
			username := "user_" + strconv.Itoa(b) + "_" + strconv.Itoa(x)
			batch.Queue("ps1", id, username, rand.Float32())
		}

		br := conn.SendBatch(ctx, batch)
		_, err := br.Exec()
		if err != nil {
			log.Fatal(err)
		}
		err = br.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

}
