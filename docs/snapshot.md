# Snapshot (checkpoint) e WAL

Este documento descreve o **ficheiro de checkpoint** (snapshot em disco), a relação com o **write-ahead log (WAL)** e como a **aplicação** configura a persistência.

## Ficheiros no diretório de dados

No mesmo diretório passado a `store.Open` (por convenção `./tmp` via `store.DefaultDataDir`):

| Ficheiro           | Função |
| ------------------ | ------ |
| `data.wal`         | Log append-only: cada `Put`/`Delete` grava um registo com sequência monótona. |
| `checkpoint.cbor`  | Snapshot opcional do estado das tabelas + último `seq` conhecido (`LastSeq`). |

Não existem ficheiros de WAL por tabela: **todas as tabelas** escrevem no mesmo `data.wal`; o nome da tabela vai dentro de cada entrada.

## Snapshot (checkpoint)

### Formato

- O snapshot é um ficheiro **CBOR** (`checkpoint.cbor`).
- Contém `Version` (actualmente `1`), `LastSeq` (`uint64`) e um mapa `Tables` com, por nome de tabela, os mapas de dados exportados (`Data`, `Fk`), como em `internal/store/snapshot.go`.

### Escrita

`Store.Checkpoint()` bloqueia a store, serializa o estado actual do `db.DB` e grava com escrita atómica: ficheiro temporário + `rename` para `checkpoint.cbor`.

O **`LastSeq` guardado** é o `nextSeq` actual da store — ou seja, o número de sequência já atribuído à última mutação registada no WAL. Qualquer entrada do WAL com `Seq <= LastSeq` é considerada **já refletida** no snapshot.

### Leitura ao abrir

Quando `store.Open` corre:

1. Cria o diretório se não existir.
2. Se existir `checkpoint.cbor`, carrega-o para memória e obtém `LastSeq`.
3. Repara o fim do WAL se estiver incompleto (`RepairTruncatesTail` em `data.wal`).
4. Abre o WAL e faz **replay** desde o início do ficheiro.

No replay, para **cada** registo lido do WAL:

- Se `entry.Seq <= LastSeq` do checkpoint, a mutação **não** é reaplicada ao `db` (já está coberta pelo snapshot).
- Caso contrário, aplica-se `Put`/`Delete` como durante operação normal.

Assim, o estado em memória combina **snapshot + apenas mutações posteriores ao checkpoint**.

### Limitações importantes

- O checkpoint **não trunca** nem roda o `data.wal`: o ficheiro de log continua a crescer. A recuperação **lê o WAL inteiro**; o ganho é **menos operações aplicadas** ao `db` durante o replay, não menos bytes lidos do disco.
- Não há checkpoint automático no código base: é preciso chamar `Checkpoint()` explicitamente (por exemplo, por política periódica ou ao encerrar).
- `LastReplayApplied` na store indica quantas entradas do WAL foram **aplicadas** no último `Open` (isto é, com `Seq > LastSeq` do checkpoint).

## WAL (write-ahead log)

### Papel

Cada `Put` e `Delete` na store:

1. Incrementa a sequência lógica e constrói um `Entry` (`Seq`, `Op`, `Table`, `Key`, `Fk`, `ValueBytes` em CBOR para `Put`).
2. **Persiste primeiro** no WAL (`WAL.Append`).
3. Só depois actualiza as tabelas em memória.

O formato do registo em disco: comprimento do payload (big-endian `uint32`), payload (magic, versão, campos), serializado `CRC32` IEEE do payload. Detalhes em `internal/store/entry.go`.

### Reprodução e reparação

- `Replay` percorre o ficheiro; dados incompletos no fim são **truncados** para o último registo válido.
- No replay, **uma entrada** cujo payload declarado exceder `2^28` octetos (256 MiB) é tratada como limite inválido e o ficheiro é truncado até ao último offset bom — ver `internal/store/wal.go`.
- `RepairTruncatesTail` pode ser usada no caminho do WAL para alinhar o fim do ficheiro antes de reabrir.

### Durabilidade (`Options`)

Configuração em `store.Options` (`internal/store/options.go`):

| Modo               | Comportamento |
| ------------------ | ------------- |
| `SyncEveryWrite`   | Após cada `Append`, faz flush do buffer (se houver) e **`fsync`** no descritor do ficheiro. Máxima durabilidade em nó único; mais lento. |
| `Buffered`         | Usa `bufio.Writer` sobre o ficheiro; dados podem ficar só em buffer até `Store.Flush()` ou `Store.Close()` (ambos fazem flush + `fsync`). Mais rápido; maior janela de perda se o processo morrer sem flush. |

`AfterSync` (opcional) é chamado **após cada `fsync` bem sucedido** — útil em testes ou métricas.

### Utilização na aplicação (`main.go`)

O binário de exemplo abre a store assim:

```go
s, err := store.Open(store.DefaultDataDir, store.Options{Durability: store.SyncEveryWrite})
```

- Diretório: `./tmp` (`DefaultDataDir`).
- Durabilidade: **`SyncEveryWrite`** — cada mutação é levada a disco de forma síncrona.

Para modo buffered com confirmação manual, usar `Durability: store.Buffered` e chamar `s.Flush()` em pontos definidos (ou confiar no `Close`).

Nem o `main` de exemplo nem a store chamam `Checkpoint()` automaticamente; para reduzir trabalho de **reaplicação** de mutações no próximo arranque (mantendo o WAL completo em disco), deve integrar-se chamadas a `s.Checkpoint()` na política da aplicação.

## Referências no código

- `internal/store/store.go` — `Open`, `Put`, `Delete`, `Checkpoint`, `Flush`, `Close`
- `internal/store/wal.go` — append, replay, truncagem de cauda
- `internal/store/snapshot.go` — formato e I/O do checkpoint
- `internal/store/entry.go` — serialização dos registos do WAL
- `internal/store/options.go` — `Durability` e `AfterSync`
