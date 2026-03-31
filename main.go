package main

import (
	"fmt"
	"log"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/store"
)

func main() {
	s, err := store.Open(store.DefaultDataDir, store.Options{Durability: store.SyncEveryWrite})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	database := s.DB()
	table := database.GetOrCreateTable("users")

	fmt.Println(table.GetByFk("user_id"))

	if err := s.Put("users", db.Item{Key: "1", Fk: "user_id", Value: "John Doe"}); err != nil {
		panic(err)
	}
	if err := s.Put("users", db.Item{Key: "2", Fk: "user_id", Value: "Jane Doe"}); err != nil {
		panic(err)
	}
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	if err := s.Delete("users", "1"); err != nil {
		panic(err)
	}
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	if err := s.Put("users", db.Item{Key: "1", Fk: "user_id", Value: "John Doe"}); err != nil {
		panic(err)
	}

	items, err := table.GetByFk("user_id")
	if err != nil {
		panic(err)
	}
	fmt.Println("items:", items)
	for _, item := range items {
		n := item.Value.(string) + "!!!"
		if err := s.Put("users", db.Item{Key: item.Key, Fk: item.Fk, Value: n}); err != nil {
			panic(err)
		}
	}

	names, err := table.GetByFk("user_id")
	if err != nil {
		panic(err)
	}
	for _, name := range names {
		fmt.Println("getByFk:", name)
	}
}
