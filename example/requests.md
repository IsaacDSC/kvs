### Create table

```sh

curl -s -X POST "http://localhost:8001/table" \
  -H "Content-Type: application/json" \
  -d '{"table_name":"test_tb"}'


```


### Creater or update data
```sh
curl -i -X PUT "http://localhost:8001/table/test_tb" \
  -H "Content-Type: application/json" \
-d '{"key": "fordel", "sk": "familia", "value":{"fordel": "fordelvalue"}}'
```

Create using optimisticLock
```sh
curl -i -X PUT "http://localhost:8001/table/test_tb?operation=optimistic_lock" \
    -H "Content-Type: application/json" \
    -d '{"key": "fordel", "sk": "familia", "value":{"keyaa": "valueaa"}, "version": {"old_version": "", "propose_version":"1"}}'
```

### Get by key 

```sh
curl -s -X GET "http://localhost:8001/table/test_tb/4321id" 
```


### Find by sk 

```sh
curl -X GET "http://localhost:8001/table/test_tb?sk=familia" | jq 
```

### Delete by key

```sh
curl -i -X DELETE "http://localhost:8001/table/test_tb" \
    -H "Content-Type: application/json" \
    -d '{"key": "fordel"}'

```
Delete using optimisticLock

```sh
curl -i -X DELETE "http://localhost:8001/table/test_tb?operation=optimistic_lock" \
  -H "Content-Type: application/json" \
  -d '{"key": "fordel", "version": {"old_version": "2"}}'

```
