# `OptimisticPut` e `OptimisticDelete`

## O que fazem

Leem o valor atual da chave, comparam **`Item.Version`** que trazes com a **`Version` guardada** e só então escrevem (`Set`) ou apagam (`Delete`). É **controle de concorrência optimista**: se outro cliente alterou a linha no meio, a versão já não bate e a operação **não** aplica o teu update — devolve `ErrInvalidVersion`.

Não substituem `NewSession`: cada chamada é **uma** escrita (uma entrada WAL por `Set`/`Delete` quando há store durável), sem agrupar várias operações numa única transação WAL.

## Regras (resumo)

| Situação | `OptimisticPut` | `OptimisticDelete` |
|----------|-----------------|---------------------|
| Chave inexistente | Erro (`Get` falha) | Erro (`Get` falha) |
| Valor na BD **sem** `Version` (`""`) | Grava e define `item.Version = candidate` | Apaga sem checar versão |
| Valor na BD **com** `Version` | Só aplica se `item.Version ==` versão na BD; depois grava com `item.Version = candidate` | Só apaga se `item.Version ==` versão na BD |

Conflito: `dbItem.Version != item.Version` → `OptimisticResult` com `Err() == ErrInvalidVersion` e `item` no resultado é o **estado atual** na tabela (útil para reler e decidir).

## `OptimisticResult`

- `Err()`: `nil` se aplicou; `ErrInvalidVersion` em conflito; outros erros (ex. chave inexistente).
- `GetLastVersion()`: quando há `ErrInvalidVersion`, devolve o `Item` atual na BD para nova tentativa ou UI.

## Exemplo

```go
cur, err := table.Get("k")
if err != nil {
    return err
}

res := table.OptimisticPut(ctx, Item{
    Key: cur.Key, Fk: cur.Fk,
    Value: "novo valor",
    Version: cur.Version, // tens de ir buscar a versão que lês
}, "v2") // próxima versão após sucesso

if err := res.Err(); errors.Is(err, memdb.ErrInvalidVersion) {
    last, _ := res.GetLastVersion()
    // alguém alterou antes: last tem o valor/versão atuais
    _ = last
    return err
}
if err != nil {
    return err
}
// sucesso: res não expõe o item final; volta a fazer Get se precisares
```

Para apagar com versão:

```go
cur, _ := table.Get("k")
res := table.OptimisticDelete(ctx, Item{Key: cur.Key, Version: cur.Version}, "v3")
if errors.Is(res.Err(), memdb.ErrInvalidVersion) { /* reler e repetir */ }
```
