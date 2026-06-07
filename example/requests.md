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
-d '{"key": "fordel", "sk": "familia", "value":{"keyaa": "valueaa", "isOk": "TRUE", "etc": [1, 2, 3, 4, 5, 6, 7]}, "version": {"old_version": "1", "propose_version":"2"}}'


```

### Bulk create or update (batch insert)

Insere/atualiza vários itens numa única requisição. O corpo é um **array** de itens (mesmo formato do PUT individual). O líder aplica o lote localmente e propõe um único log `BulkPutCmd` ao Raft.

```sh
curl -i -X PUT "http://localhost:8001/table/test_tb/operation/bulk" \
  -H "Content-Type: application/json" \
  -d '[
    {"key": "fordel", "sk": "familia", "value": {"fordel": "fordelvalue"}},
    {"key": "isaac", "sk": "familia", "value": {"age": 29, "tags": [1, 2, 3]}},
    {"key": "raquel", "sk": "familia", "value": {"age": 26, "tags": [1, 2, 3]}},
    {"key": "gabi", "sk": "familia", "value": {"age": 27, "tags": [1, 2, 3]}},
    {"key": "ratilson", "sk": "familia", "value": {"age": 31, "tags": [1, 2, 3]}},
    {"key": "vick", "sk": "familia", "value": {"age": 27, "tags": [1, 2, 3]}},
    {"key": "amanda", "sk": "familia", "value": {"age": 36, "tags": [1, 2, 3]}},
    {"key": "adriana", "sk": "familia", "value": {"age": 50, "tags": [1, 2, 3]}},
    {"key": "carlos", "value": {"active": true}}
  ]'
```

Com quorum explícito (`raft_min_acks` em [maioría, N]):

```sh
curl -i -X PUT "http://localhost:8001/table/test_tb/operation/bulk?raft_min_acks=2" \
  -H "Content-Type: application/json" \
  -d '[
    {"key": "k1", "value": {"x": 1}},
    {"key": "k2", "sk": "s", "value": {"y": 2}}
  ]'
```

> Cada item exige `key` e `value`. Se algum item for inválido, o lote inteiro é rejeitado com `422 Unprocessable Entity` e nada é persistido.

### Get by key 

```sh
curl -s -X GET "http://localhost:8001/table/test_tb/fordel" 
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

### Raft quorum options on writes (leader only)

Parâmetros opcionais (PUT/DELETE em query; POST em JSON), além do payload do item/delete:

- **Sem `raft_min_acks` / `min_acks` (ou valor `0`)**: o servidor substitui por **replicação em todo o cluster (N ACKs)** neste pedido antes de propor ao Raft.
- **`raft_min_acks` positivo**: obriga **esta** entrada a um mínimo próprio de ACKs antes do commit (deve estar em [maioría, N]).

Eleições Raft continuam a usar **maioría**.

Exemplos (cluster de **3** nós → N = 3, maioría = 2):

```sh
curl -s -X POST "http://localhost:8001/table" \
  -H "Content-Type: application/json" \
  -d '{"table_name":"test_tb"}'
```

PUT sem query (`raft_min_acks` omitido → **N**):

```sh
curl -i -X PUT "http://localhost:8001/table/test_tb" \
  -H "Content-Type: application/json" \
  -d '{"key": "fordel", "sk": "familia", "value":{"fordel": "fordelvalue"}}'
```

PUT equivalente explícito ao default de 3‑de‑3:

```sh
curl -i -X PUT "http://localhost:8001/table/test_tb?raft_min_acks=3" \
  -H "Content-Type: application/json" \
  -d '{"key": "fordel", "sk": "familia", "value":{"fordel": "fordelvalue", "v": [1,2,3]} }'
```

Consultar `MajorityRepMinAcks` e `EffectiveRepMinAcks` (sempre N para entradas com `MinAcks` omitido no caminho Raft) em `GET /state`.

