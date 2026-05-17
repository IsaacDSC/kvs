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
	"time"

	sdk "github.com/IsaacDSC/kvs/SDK"
	"github.com/jaswdr/faker/v2"
	"github.com/stretchr/testify/assert"
)

func getLeaderAndFollowers(t *testing.T, tableName string) (*sdk.Table, []*sdk.Table) {
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

	return tableLeader, tableFollowers
}

func TestMultipleNodes(t *testing.T) {
	fake := faker.New()
	tableName := fmt.Sprintf("sdk_multiple_nodes_test:%s", fake.Hash().MD5())
	tableLeader, tableFollowers := getLeaderAndFollowers(t, tableName)

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
				time.Sleep(time.Millisecond * 100)
				for _, tb := range tableFollowers {
					fItems, err := tb.FindAll(context.Background(), input[0].SK)
					assert.NoError(t, err)
					assert.Equal(t, input, fItems)
				}
			})

		})

		t.Run("Test find peers validate to consistency", func(t *testing.T) {
			time.Sleep(time.Millisecond * 100)
			for _, i := range input {
				item, err := tableLeader.Find(context.Background(), i.Key)
				assert.NoError(t, err)
				assert.Equal(t, i, item)

				for _, tb := range tableFollowers {
					fItem, err := tb.Find(context.Background(), i.Key)
					assert.NoError(t, err)
					assert.Equal(t, i, fItem)
				}
			}
		})

		t.Run("Expected delte in follower with error", func(t *testing.T) {
			for _, tb := range tableFollowers {
				for _, i := range input {
					assert.Error(t, tb.Del(context.Background(), i.Key))
				}
			}
		})

		t.Run("Expected delete all items and finaly is consistent", func(t *testing.T) {
			time.Sleep(time.Millisecond * 100)
			for _, i := range input {
				assert.NoError(t, tableLeader.Del(context.Background(), i.Key))
				_, err := tableLeader.Find(context.Background(), i.Key)
				assert.Error(t, err)
				time.Sleep(time.Millisecond * 100)
				for _, tb := range tableFollowers {
					_, err := tb.Find(context.Background(), i.Key)
					assert.Error(t, err)
				}
			}
		})

	})

	t.Run("Test writter in follower", func(t *testing.T) {
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
