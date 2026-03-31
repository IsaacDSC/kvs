package main

import (
	"context"
	"fmt"
	"time"

	"github.com/IsaacDSC/kvs/internal/db"
)

func main() {
	database := db.NewDB()
	table := database.CreateTable("users")
	table.Set(db.Item{Key: "1", Fk: "user_id", Value: "John Doe"})
	table.Set(db.Item{Key: "2", Fk: "user_id", Value: "Jane Doe"})
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	table.Delete("1")
	fmt.Println(table.Get("1"))
	fmt.Println(table.GetByFk("user_id"))
	table.Set(db.Item{Key: "1", Fk: "user_id", Value: "John Doe"})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := table.NewSession(ctx, func(tx *db.Tx) error {
		items := tx.GetByFk("user_id")
		fmt.Println("items:", items)

		time.Sleep(100 * time.Millisecond)
		for _, item := range items {
			n := item.Value.(string)
			n = n + "!!!"
			tx.Set(db.Item{Key: item.Key, Fk: item.Fk, Value: n})
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

	names, err := table.GetByFk("user_id")
	if err != nil {
		panic(err)
	}
	for _, name := range names {
		fmt.Println("getByFk:", name)
	}
}
