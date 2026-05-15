### Create table

```sh

curl -s -X POST "http://localhost:8001/table" \
  -H "Content-Type: application/json" \
  -d '{"table_name":"test_tb"}'


```


### Creater or update data
```sh
curl -s -X PUT "http://localhost:8001/table/test_tb/4321id" \
  -H "Content-Type: application/json" \
  -d '{"sk": "familia", "value":{"key2": "value2"}}'
```

Create using optimisticLock
```sh
curl -s -X PUT "http://localhost:8001/table/test_tb/4321id?operation=optimistic_lock" \
    -H "Content-Type: application/json" \
    -d '{"sk": "familia", "value":{"key2": "value2"}, "version": {"old_version": "", "propose_version":"1"}}'
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
curl -i -X DELETE "http://localhost:8001/table/test_tb/4321id" 

```
