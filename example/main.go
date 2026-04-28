package main

import (
	"fmt"
	"log"
	"os"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/store"
)

func main() {
	dataDir, err := os.MkdirTemp("", "kvs-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dataDir) }()

	s, err := store.Open(dataDir, store.Options{Durability: store.SyncEveryWrite})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	users := s.AppDB().GetOrCreateTable("users")

	if err := users.Set(db.Item{Key: "1", Fk: "team-a", Value: "Alice"}); err != nil {
		log.Fatal(err)
	}
	if err := users.Set(db.Item{Key: "2", Fk: "team-a", Value: "Bob"}); err != nil {
		log.Fatal(err)
	}

	one, err := users.Get("1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Get(1):", one)

	teamA, err := users.GetByFk("team-a")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("GetByFk(team-a):", teamA)

	if err := users.Delete("1"); err != nil {
		log.Fatal(err)
	}

	afterDelete, err := users.GetByFk("team-a")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("After Delete(1):", afterDelete)
}
