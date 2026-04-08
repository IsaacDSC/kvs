package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/store"
)

func main() {
	s, err := store.Open(store.DefaultDataDir, store.Options{Durability: store.SyncEveryWrite})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	database := s.AppDB()
	table := database.GetOrCreateTable("users")

	fmt.Println(table.GetByFk("user_id"))

	if err := table.Set(db.Item{Key: "1", Fk: "user_id", Value: "John Doe"}); err != nil {
		panic(err)
	}
	if err := table.Set(db.Item{Key: "2", Fk: "user_id", Value: "Jane Doe"}); err != nil {
		panic(err)
	}
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	if err := table.Delete("1"); err != nil {
		panic(err)
	}
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	if err := table.Set(db.Item{Key: "1", Fk: "user_id", Value: "John Doe"}); err != nil {
		panic(err)
	}

	items, err := table.GetByFk("user_id")
	if err != nil {
		panic(err)
	}
	fmt.Println("items:", items)
	for _, item := range items {
		n := item.Value.(string) + "!!!"
		if err := table.Set(db.Item{Key: item.Key, Fk: item.Fk, Value: n}); err != nil {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = table.NewSession(ctx, func(tx *db.Tx) error {
		item, err := tx.Get("1")
		if err != nil {
			return err
		}

		// time.Sleep(15 * time.Second)
		return tx.Set(item)
	})
	if err != nil {
		panic(err)
	}

	item, err := table.Get("1")
	if err != nil {
		panic(err)
	}

	item.Value = "Joahna Maria"
	optCtx := context.Background()
	result := table.OptimisticPut(optCtx, item, "1")
	if err := result.Err(); err != nil {
		panic(err)
	}

	item, err = table.Get("1")
	if err != nil {
		panic(err)
	}

	item.Version = "2"
	item.Value = "Joahna Maria2"
	result2 := table.OptimisticPut(optCtx, item, "3")
	err = result2.Err()
	if err != nil {
		lastVersion, err := result2.GetLastVersion()
		fmt.Println("expected error version mismatch:", lastVersion, err)
		// panic(err)
	}

	delResult := table.OptimisticDelete(ctx, item, "3")
	if err := delResult.Err(); err != nil {
		lastVersion, err := result2.GetLastVersion()
		fmt.Println("expected error Del version mismatch:", lastVersion, err)
		// panic(err)
	}

	it, err := table.Get("1")
	if err != nil {
		panic(err)
	}

	fmt.Println("it:", it)

}
