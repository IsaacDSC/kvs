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

### Get by key 

```sh
curl -s -X GET "http://localhost:8001/table/test_tb/1234id" 
```


### Find by sk 

```sh
curl -i -X GET "http://localhost:8001/table/test_tb?sk=123" 
```

### Delete by key

```sh
curl -i -X DELETE "http://localhost:8001/table/test_tb/123" 

```