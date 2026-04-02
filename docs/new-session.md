# `Table.NewSession`

## O que faz

`NewSession` executa `fn(tx *Tx)` com **um lock exclusivo por tabela**: enquanto uma sessão corre, outra na **mesma** `Table` não intercala.

Dentro de `fn`, `tx.Set` e `tx.Delete` **não** gravam logo no WAL/memória final: ficam em **buffer**. Só depois de `fn` retornar **sem erro** e o **contexto** ainda válido é feito o **commit** (WAL `BEGIN` → mutações → `COMMIT`, se o store durável suportar; senão aplica só em memória).

Se `fn` falhar, o contexto cancelar/estourar tempo, ou o commit falhar → **nenhuma** mutação dessa sessão é persistida (rollback lógico).

## ACID (o que esta API garante)

| Letra | Garantia neste projeto |
|--------|-------------------------|
| **A** — Atomicidade | Todas as operações da sessão entram no disco/memória **juntas** ou **nenhuma** (não há WAL “a meio” sem `COMMIT` no fluxo transacional). |
| **C** — Consistência | Regras de negócio são **tuas** (validação em `fn`). A tabela garante encode/decode e índice `Fk` como no resto da API. |
| **I** — Isolamento | **Serial por tabela**: uma sessão de cada vez por `Table`; dentro da sessão, `Get`/`GetByFk` veem o estado **já alterado** pelo próprio `tx` (lista ordenada de mutações). Não há isolamento entre **tabelas** diferentes numa única sessão (a sessão é por `Table`). |
| **D** — Durabilidade | Com `store.Open`, o grupo fica no WAL no commit; **força** depende de `Options.Durability` (`SyncEveryWrite` vs `Buffered` + `Flush`). Sem store durável, só memória de processo. |

## Exemplo

```go
table := database.GetOrCreateTable("users") // *db.DB após store.Open, etc.

ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

err := table.NewSession(ctx, func(tx *db.Tx) error {
    item, err := tx.Get("1")
    if err != nil {
        return err
    }

    // time.Sleep(15 * time.Second)
    return tx.Set(item)
})

if err != nil {
    panic(err)
}

```

Com `store.Open`, `Table.Set`/`Delete` **fora** de `NewSession` continuam a ser **uma** entrada WAL por chamada; só `NewSession` agrupa em **uma** transação WAL.
