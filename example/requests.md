### Create table

```sh

curl -s -X POST "http://localhost:8001/table" \
  -H "Content-Type: application/json" \
  -d '{"table_name":"test_tb"}'


```


### Creater or update data
```sh
curl -s -X PUT "http://localhost:8001/table/test_tb/123" \
  -H "Content-Type: application/json" \
  -d '{"sk": "123", "value":{"key": "value"}}'
```

### Get by key 

```sh
curl -s -X GET "http://localhost:8001/table/test_tb/123" 
```


### Find by sk 

```sh
curl -i -X GET "http://localhost:8001/table/test_tb?sk=123" 
```