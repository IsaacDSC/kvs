package sdk_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"

	sdk "github.com/IsaacDSC/kvs/SDK"
	"github.com/jaswdr/faker/v2"
	"github.com/stretchr/testify/assert"
)

func TestMultipleNodes(t *testing.T) {
	fake := faker.New()
	peers := [3]string{
		"http://localhost:8001",
		"http://localhost:8002",
		"http://localhost:8003",
	}

	var (
		wg     sync.WaitGroup
		leader string
	)
	// Get leader

	for _, h := range peers {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			// curl +X GET {host}/state
			resp, err := http.Get(fmt.Sprintf("%s/state", host))
			assert.NoError(t, err)
			assert.Equal(t, resp.StatusCode, 200)
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			fmt.Println(string(b))
			var output struct{ Role string }
			assert.NoError(t, json.Unmarshal(b, &output))
			permitted := []string{"follower", "leader"}
			assert.Contains(t, permitted, output.Role)
			if output.Role == "leader" {
				leader = host
			}
		}(h)
	}

	wg.Wait()
	assert.NotEmpty(t, leader)
	assert.Contains(t, peers, leader)
	fmt.Println(leader)

	tableName := fmt.Sprintf("sdk_multiple_nodes_test:%s", fake.Hash().MD5())
	tableLeader, err := sdk.GetOrCreateTable(leader, tableName)
	if err != nil {
		log.Fatalf("error on GetOrCreateTable: leader with error : %v", err)
	}

	var tableFollowers []*sdk.Table
	var followers []string
	for _, h := range peers {
		if h == leader {
			continue
		}
		followers = append(followers, h)
		t, err := sdk.GetOrCreateTable(h, tableName)
		if err != nil {
			log.Fatalf("error on GetOrCreateTable: follower with error : %v", err)
		}
		tableFollowers = append(tableFollowers, t)
	}

	t.Run("Test writter leader", func(t *testing.T) {
		skGroup := fake.Hash().MD5()
		input := []sdk.Item{
			{
				Key:     fake.Hash().MD5(),
				SK:      skGroup,
				Value:   map[string]any{"name": fake.Person().Name()},
				Version: "1",
			},
			{
				Key:     fake.Hash().MD5(),
				SK:      skGroup,
				Value:   map[string]any{"name": fake.Person().Name()},
				Version: "1",
			},
		}

		for _, item := range input {
			inputItem := sdk.NewItem().
				WithKey(item.Key).
				WithSk(item.SK).
				WithValue(item.Value).
				WithVersion("", item.Version).
				Build()

			err := tableLeader.Set(context.Background(), inputItem)
			assert.NoError(t, err)
		}

		t.Run("Test findAll peers validate to consistency", func(t *testing.T) {
			t.Run("Expected leader consistent", func(t *testing.T) {
				items, err := tableLeader.FindAll(context.Background(), input[0].SK)
				assert.NoError(t, err)
				assert.Equal(t, input, items)
			})

			t.Run("Expected followers consistent", func(t *testing.T) {
				for _, tb := range tableFollowers {
					fItems, err := tb.FindAll(context.Background(), input[0].SK)
					assert.NoError(t, err)
					assert.Equal(t, input, fItems)
				}
			})

		})

		t.Run("Test find peers validate to consistency", func(t *testing.T) {
			t.Skip()
			for _, i := range input {
				item, err := tableLeader.Find(context.Background(), i.Key)
				assert.NoError(t, err)
				assert.Equal(t, input, item)

				for _, tb := range tableFollowers {
					fItem, err := tb.Find(context.Background(), i.Key)
					assert.NoError(t, err)
					assert.Equal(t, input, fItem)
				}
			}
		})
	})

	t.Run("Test writter in follower", func(t *testing.T) {
		t.Skip()
		item := sdk.Item{
			Key:     fake.Hash().MD5(),
			SK:      fake.Hash().MD5(),
			Value:   map[string]any{"name": fake.Person().Name()},
			Version: "1",
		}

		inputItem := sdk.NewItem().
			WithKey(item.Key).
			WithSk(item.SK).
			WithValue(item.Value).
			WithVersion("", item.Version).
			Build()

		for _, tb := range tableFollowers {
			err := tb.Set(context.Background(), inputItem)
			assert.Error(t, err)
		}
	})

}

func TestFlow(t *testing.T) {

	fake := faker.New()

	tb, err := sdk.GetOrCreateTable("http://localhost:8001", "sdk_test_app")
	assert.NoError(t, err)

	t.Run("Test Create, Update With Optimistic and Find", func(t *testing.T) {
		k := fake.Hash().MD5()
		input := sdk.Item{
			Key:     k,
			SK:      fake.Hash().MD5(),
			Value:   map[string]any{"name": fake.Person().Name()},
			Version: "1",
		}

		inputItem := sdk.NewItem().
			WithKey(input.Key).
			WithSk(input.SK).
			WithValue(input.Value).
			WithVersion("", input.Version).
			Build()

		err = tb.Set(context.Background(), inputItem)
		assert.NoError(t, err)

		item, err := tb.Find(context.Background(), k)
		assert.NoError(t, err)

		assert.Equal(t, input, item)

		input.Value = map[string]any{"name": fake.Person().Name()}
		input.Version = "2"
		updateInputItem := sdk.NewItem().
			WithKey(input.Key).
			WithSk(input.SK).
			WithValue(input.Value).
			WithVersion("1", "2").
			Build()

		err = tb.Set(context.Background(), updateInputItem)
		assert.NoError(t, err)

		item2, err := tb.Find(context.Background(), k)
		assert.NoError(t, err)

		assert.Equal(t, input, item2)

		assert.NoError(t, tb.Del(context.Background(), input.Key))
	})

	t.Run("Test Create and FindAll", func(t *testing.T) {
		skGroup := fake.Hash().MD5()
		input := []sdk.Item{
			{
				Key:     fake.Hash().MD5(),
				SK:      skGroup,
				Value:   map[string]any{"name": fake.Person().Name()},
				Version: "1",
			},
			{
				Key:     fake.Hash().MD5(),
				SK:      skGroup,
				Value:   map[string]any{"name": fake.Person().Name()},
				Version: "1",
			},
		}

		for _, item := range input {
			inputItem := sdk.NewItem().
				WithKey(item.Key).
				WithSk(item.SK).
				WithValue(item.Value).
				WithVersion("", item.Version).
				Build()

			err = tb.Set(context.Background(), inputItem)
			assert.NoError(t, err)
		}

		items, err := tb.FindAll(context.Background(), skGroup)
		assert.NoError(t, err)
		assert.Equal(t, len(input), len(items))
		for i, item := range items {
			assert.Equal(t, input[i], item)
			assert.NoError(t, tb.Del(context.Background(), input[i].Key))
		}
	})

}
