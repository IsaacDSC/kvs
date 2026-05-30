package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Table struct {
	host   string
	name   string
	client http.Client
}

/*
	curl -s -X POST "http://localhost:8001/table" \
	  -H "Content-Type: application/json" \
	  -d '{"table_name":"test_tb"}'
*/
func GetOrCreateTable(host, tableName string) (*Table, error) {
	body, _ := json.Marshal(map[string]string{"table_name": tableName})
	resp, err := http.Post(host+"/table", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	// Server returns 201 Created on success; followers may reject with a body mentioning the leader.
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		break
	default:
		if !strings.Contains(string(b), "follower") {
			return nil, fmt.Errorf("create table: status %d: %s", resp.StatusCode, b)
		}
	}

	return &Table{host: host, name: tableName}, nil
}

/*
	curl -i -X PUT "http://localhost:8001/table/test_tb?operation=optimistic_lock" \
	    -H "Content-Type: application/json" \
	    -d '{"key": "fordel", "sk": "familia", "value":{"keyaa": "valueaa", "etc": [1, 2, 3, 4, 5]}, "version": {"old_version": "", "propose_version":"1"}}'
*/
func (t Table) Set(ctx context.Context, it *inputItem) error {
	endpoint := t.host + "/table/" + t.name
	if it.version != nil {
		endpoint += "?operation=optimistic_lock"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(it.Json()))
	if err != nil {
		return fmt.Errorf("set: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set: status %d: %s", resp.StatusCode, b)
	}
	return nil
}

/*
curl -s -X GET "http://localhost:8001/table/test_tb/fordel"
*/
func (t Table) Find(ctx context.Context, key string) (Item, error) {
	endpoint := t.host + "/table/" + t.name + "/" + url.PathEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Item{}, fmt.Errorf("find: build request: %w", err)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return Item{}, fmt.Errorf("find: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return Item{}, fmt.Errorf("find: status %d: %s", resp.StatusCode, b)
	}
	var item Item
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return Item{}, fmt.Errorf("find: decode: %w", err)
	}
	return item, nil
}

/*
curl -X GET "http://localhost:8001/table/test_tb?sk=familia" | jq
*/
func (t Table) FindAll(ctx context.Context, sk string) ([]Item, error) {
	endpoint := t.host + "/table/" + t.name + "?sk=" + url.QueryEscape(sk)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("find all: build request: %w", err)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("find all: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("find all: status %d: %s", resp.StatusCode, b)
	}
	var items []Item
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("find all: decode: %w", err)
	}
	return items, nil
}

/*
	curl -i -X DELETE "http://localhost:8001/table/test_tb?operation=optimistic_lock" \
	 -H "Content-Type: application/json" \
	 -d '{"key": "fordel", "version": {"old_version": "2"}}'
*/
func (t Table) Del(ctx context.Context, key string) error {
	endpoint := t.host + "/table/" + t.name
	body, _ := json.Marshal(map[string]string{"key": key})
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("del: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("del: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("del: status %d: %s", resp.StatusCode, b)
	}
	return nil
}
