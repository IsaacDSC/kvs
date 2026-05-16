# Consistência: checkpoint do WAL vs materialização no `fsdb`

## Origem do problema

O refinamento [refining/consistence_checkpoint_wal_and_fsdb.md](../refining/consistence_checkpoint_wal_and_fsdb.md) descreve falhas como `open tmp/node1/test_tb/key/fordel: no such file or directory` durante **flush**/materialização — ou seja, existe uma janela em que **metadados** (`LastSeq` / WAL) ou leituras assumem caminhos no `fsdb` que ainda **não** existem, ou vice-versa relativamente ao batcher diferido (`DeferWrites`).

Este documento **não** substitui [specs/checkpoint-wal-recovery.md](./checkpoint-wal-recovery.md) nem [specs/reducao-pressao-filesystem-batch-merge.md](./reducao-pressao-filesystem-batch-merge.md); fixa dois desenhos combináveis: **B — Seq consolidado (“congelado”) no checkpoint** e **E — endurecimento de caminhos no `fsdb`**.

## Contexto no código atual (âncoras)

| Peça | Papel relevante |
|------|-----------------|
| `internal/wal.WAL.Checkpoint` | `Flush` do WAL → `Options.BeforeCheckpoint` (ex.: `WriteBatcher.Flush`) → `durable.SaveLastSeq` com `w.seq` (ver `internal/wal/wal.go`). |
| `internal/wal.Options` | `BeforeCheckpoint` documentado para flush de armazenamento diferido; `CheckpointConfig` reserva `EveryNWrites`/`MaxWalBytes` com nota explícita sobre coordenação com `fsdb`. |
| `cmd/node` | Liga `BeforeCheckpoint` ao `WriteBatcher.Flush` quando `FSDeferWrites` está ativo. |
| `internal/fsdb.Db` | `CreateTable` cria `table/`, `table/key/`, `table/sk/`. `Set` faz `WriteFile` em `table/key/<pk>` **sem** `MkdirAll` — assume que `CreateTable` correu (ou replay que chama `CreateTable` antes de `Set`). |
| `internal/fsdb.TbMutex` | `Lock`/`RLock` **criam** mutex por nome de tabela se ainda não existir; **não** criam diretórios no disco. |
| `internal/fsdb.WriteBatcher` | Com `DeferWrites`, `Set`/`Del` podem só enfileirar; `Flush` aplica o mapa LWW inteiro ao `inner`. `CreateTable` vai direto ao `inner`. |
| `internal/db.Adapter` | `Set`/`Delete` tomam `mu` por pedido: ordem **WAL → fsdb (batcher) → memdb** dentro desse lock. |
| `internal/wal.WAL.Load` | Por entrada, chama `CreateTable` antes de `Set`/`Del` no target. |

## Hipóteses de causa (relevantes para B e E)

1. **Corte lógico inconsistente entre WAL e `fsdb`**  
   O checkpoint grava um `Seq` (“até onde o WAL está consolidado”) que pode **não corresponder** ao conjunto exacto de mutações já aplicadas ao `inner` quando existe **i)** batcher diferido; **ii)** `SaveLastSeq` baseado num `w.seq` lido noutra fase da função sem um **único corte** alinhado com o flush do batcher.

2. **Janela `SaveLastSeq(w.seq)` vs `BeforeCheckpoint`**  
   Se após flush do WAL e do batcher o valor persistido continuar a ser **`w.seq` no momento do `SaveLastSeq`**, escritas concurrentes podem incrementar `w.seq` ou em alternativa deixarem operações apenas no WAL/memdb sem o mesmo corte temporal no disco — vide secção sobre **ordenação real** mais abaixo.

3. **Ordenação real: WAL concluído ≠ mutação já no batcher**  
   Um `Wal.Set` retorna assim que o frame está escrito no ficheiro; no `Adapter`, o mesmo request continua sob `mu` para enfileirar no batcher — **boa** ordenação dentro de um único pedido. O problema aparece quando **checkpoint** pode correr **sem** partilhar o mesmo `Adapter.mu`, permitindo intercalar outro pedido completo ou parte do pipeline com o checkpoint.

4. **`ENOENT` sem `CreateTable` efectivo no disco**  
   `TbMutex` pode permitir concorrência por tabela sem os directórios terem sido criados (p.ex. caminhos que contornam `CreateTable` ou bugs de ordem externa). `Set` falha com “no such file or directory” no `WriteFile`.

## Invariante alvo

Alinhado a [checkpoint-wal-recovery.md](./checkpoint-wal-recovery.md):

- Após gravar checkpoint com `LastSeq = N`, o estado durável do `fsdb` deve refletir **todas** as mutações com `Seq ≤ N` que o contrato de replicação considera “do tail”.

Com batcher diferido ([reducao-pressao-filesystem-batch-merge.md](./reducao-pressao-filesystem-batch-merge.md)):

- `LastSeq = N` só pode ser persistido **depois** de um flush do batcher que inclua **exactamente** o fecho do conjunto de mutações do `fsdb` correspondentes a `Seq ≤ N` (no desenho **B**, isso implica serializar com o pipeline que produz essas mutações).

---

## B. Seq “congelado” no checkpoint + corte consistente com o `fsdb`

### Objetivo semântico

Introduzir um **único valor** `checkpointSeq` tal que:

1. Todo o conteúdo necessário do WAL até `checkpointSeq` está **visível no ficheiro WAL** (buffer esvaziado / `sync` conforme `Durability`).
2. O `fsdb` **inner** (atrás do batcher, se existir) reflete **todas** as mutações do `fsdb` cujo `Seq` no WAL é `≤ checkpointSeq`.
3. `durable.SaveLastSeq(dir, checkpointSeq)` usa **esse** valor, não um `w.seq` “lido mais tarde” sem relação com o flush.

**Nota:** “congelar” aqui significa **fixar o número de sequência do corte** usado no metadado; não implica parar o relógio do WAL para sempre — apenas que o ficheiro de checkpoint não avança além do corte cuja materialização foi verificada.

### Porque `SaveLastSeq(w.seq)` no fim da função é insuficiente

Situações a eliminar:

- **Corrida só de leitura de `seq`**: entre `BeforeCheckpoint()` e `SaveLastSeq(...)`, outros pedidos podem completar novos appends WAL; usar `w.seq` “ao final” promove um `LastSeq` possivelmente **adiantado** relativamente ao que o último flush do batcher efetivamente cobriu quando o trabalho não está serializado (ver seguinte bullet).
- **Flush total do batcher sem corte temporal**: `WriteBatcher.Flush` aplica **todo** `pend`. Se durante o checkpoint entrassem no `pend` mutações correspondentes a `Seq > checkpointSeq`, poder-se-iam materializar no disco **efeitos** que o `LastSeq` “antigo” ainda não deveria afirmar, ou **inverter-se-ia** a causalidade relativamente ao truncamento WAL (quando configurado para truncar).

Conclusão: **B exige dois ingredientes em conjunto** — (i) **`checkpointSeq` explícito** obtido na ordem certa relativamente ao `Flush` WAL; (ii) **exclusão mútua** entre o período que calcula/`flush`/grava esse corte e a orquestração **WAL + enfileiramento/materialização no `fsdb`** que define o que pertence ao corte “fechado”.

### Implementação recomendada (prioridade): barreira sob `internal/db.Adapter`

O `Adapter` já serializa **`Wal.Set` / `Wal.Delete`** com **`Adapter.mu`** e só depois **`fsdb` (batcher) → `memdb`**. Essa propriedade é exactamente o **“ponto único”** onde o WAL e o batcher ficam causalmente ordenados entre pedidos.

**Desenho:**

1. Introduzir uma operação **`Adapter.Checkpoint`(ou método equivalente)** invocável a partir do `cmd/node` no lugar de **`wal.Checkpoint`** directo quando o `logdb` for o WAL e existir checkpoint configurado.
2. Pseudocódigo (comportamento, não assinatura final):

```text
func (f *Adapter) CheckpointData(ctx context.Context) error {
    f.mu.Lock()
    defer f.mu.Unlock()

    // w := f.logdb como *wal.WAL (por interface estreita ou tipo concreto injetado)
    if err := w.Flush(); err != nil { return err }

    checkpointSeq := w.Seq() // ver nota “API do WAL” abaixo

    if hook := w.BeforeCheckpointHook(); hook != nil {
        if err := hook(ctx); err != nil { return err }
    }

    if err := durable.SaveLastSeq(w.CheckpointDir(), checkpointSeq); err != nil { return err }

    if w.TruncateAfterCheckpoint() {
        if err := w.TruncateAfterSave(); err != nil { return err }
    }
    return nil
}
```

3. O **goroutine** periódico (`tasks.RunPeriodicWALCheckpoint`) deve chamar **`Adapter.CheckpointData`** (ou wrapper que a recebe), **não** `wal.WAL.Checkpoint` isolado.

**Porque funciona:** enquanto `f.mu` está detido, **não** há `Set`/`Delete` a avançar `w.seq` nem a enfileirar no batcher; o flush WAL vê um `seq` estável; o `BeforeCheckpoint` (flush batcher) drena o `pend` produzido **apenas** por pedidos já completos antes do lock do checkpoint; `checkpointSeq` corresponde ao último frame WAL integrado nesse estado.

**Nota sobre `Load` / `post-load checkpoint`:** o arranque chama `database.Load` antes de servir tráfego; aqui a concorrência é usualmente reduzida. Continua a ser correcto passar pelo mesmo caminho (`Adapter` bloqueado ou caminho equivalente) para o **primeiro** checkpoint pós-recuperação, para não reintroduzir a corrida em testes que disparam API cedo.

### API mínima no pacote `wal` para suportar B sem acoplar `durable` ao `Adapter`

Hoje `Checkpoint()` encapsula `SaveLastSeq` e `truncate`. Para manter [checkpoint-wal-recovery.md](./checkpoint-wal-recovery.md) (“adapter não importa `durable`”), há duas variantes:

- **B-API-1 (preferível para fronteira limpa):** factorizar `internal/wal` em:
  - `FlushForCheckpoint() error`
  - `CommittedSeq() uint64` — valor monotónico já atribuído ao último append **bem sucedido** (o `w.seq` actual após flush)
  - `RunBeforeCheckpoint(ctx) error` — delega em `opts.BeforeCheckpoint`
  - `SaveCheckpointMetadata(dir string, seq uint64) error` + `MaybeTruncate() error`  
  O `Adapter` orquestra a sequência **ou** um método `WAL.CheckpointFrozen()` que aceita `externallyLocked bool` e documenta que **deve** ser chamado com exclusão no `Adapter` quando `BeforeCheckpoint` materializa outro backend.

- **B-API-2:** manter `WAL.Checkpoint` mas documentar e **forçar** em testes que, com `BeforeCheckpoint` ≠ nil, **só** pode ser chamado sob lock do `Adapter` (frágil se alguém chamar directamente o WAL).

O projecto deve escolher **uma** fronteira; a spec recomenda **B-API-1** com testes que falham se `Checkpoint` “antigo” for usado com batcher diferido sem lock.

### Congelar `checkpointSeq`: onde ler `w.seq`

Ordem obrigatória **dentro** da secção crítica que serializa com o `Adapter`:

1. `WAL.Flush()` — garante que todo o buffer do WAL (se `Buffered`) está no ficheiro e `sync` aplicado conforme política.
2. `checkpointSeq := w.seq` — imediatamente a seguir, **ainda** sob o mesmo lock que impede novos `Wal.Set`.

**Não** mover a leitura de `w.seq` para depois de `BeforeCheckpoint` sem manter o lock: isso reabre a corrida com novos appends.

### Interacção com `TruncateAfterCheckpoint`

Truncar o WAL imediatamente a seguir a `SaveLastSeq(checkpointSeq)` é coerente com “o ficheiro WAL só precisa de sufixo com `Seq > checkpointSeq`”.

Requisitos adicionais:

- O truncamento deve ocorrer **na mesma zona de exclusão** que impediu novos appends inconsistentes — tipicamente ainda dentro de `Adapter.CheckpointData` antes de libertar `mu`.

- Após truncar, o WAL deve continuar a emitir **`Seq > checkpointSeq` monotónico** (comportamento já descrito nos comentários de `TruncateAfterCheckpoint` em `wal.Options`), e `Load` continua com `max(lastSeq, replay)`.

### `context.Context`

`Wal.Checkpoint` hoje usa `context.Background()` no `BeforeCheckpoint`. A operação através do `Adapter` deve propagar **`ctx`** até ao hook (`Flush` no batcher) para cancelamento e timeouts alinhados com `cmd/node`.

### Critérios de aceite específicos de B

- Com `DeferWrites=true`, checkpoint periódico ativo e carga concurrent, **reinício** + `Load` reproduzem o mesmo estado visível para o conjunto esperado até `LastSeq` (replay parcial só do sufixo).
- Ensaio **`go test -race`** nos pacotes tocados quando houver paralelismo entre checkpoint e escritas HTTP/tarefas em teste de integração.
- Qualquer novo caminho público ao `Wal.Checkpoint` directo sob `BeforeCheckpoint` ≠ nil **falha de revisão / teste** salvo uso documentado dentro do lock.

### Lacuna / suposição explícita

- Se à futura uma escrita WAL puder disparar-se **sem** passar pelo `Adapter` (ferramentas, testes, outro binário), **B** exige o mesmo contrato de exclusão **ou** materialização por `Seq` no batcher (fora do âmbito deste documento). A suposição operacional é: **produção** consolidada via `Adapter`.

---

## E. Endurecimento no `fsdb`: `MkdirAll` nos caminhos de dados

### Objetivo semântico

Reduzir a classe de erros `ENOENT` / `ENOTDIR` em `WriteFile` / `ReadFile` / `Remove` quando o segmento `table/key/` ou `table/sk/` **ainda não** existe, **sem** substituir a correção causal **B** para `LastSeq`.

### Onde aplicar (ficheiro `internal/fsdb/base.go`)

| Método | Momento | Acção proposta |
|--------|---------|----------------|
| `Set` | Antes de `WriteFile` em `.../key/<pk>` | `MkdirAll(filepath.Join(d.defaultDir, table, "key"), 0o755)` (ou `0o755` alinhado a `CreateTable`). |
| `Set` | Antes de `WriteFile` em `.../sk/<sk>` quando `data.SK != ""` | `MkdirAll(filepath.Join(d.defaultDir, table, "sk"), 0o755)`. |
| `Del` | Antes de reescrever `.../sk/<sk>` após remoção de PK | Garantir `.../sk/` existente (idempotente). |
| `Get` / `GetBySk` | Opcional | Normalmente **não** criar pastas em leitura; se no futuro `GetBySk` materializar índice, reavaliar. |

**Permissões:** usar as mesmas máscaras que `CreateTable` (`0755` / `0o755`) para evitar surpresas em ambientes com `umask`.

### Interacção com `writeBlockedByMissingPath` e `db.ErrTableNotFound`

Hoje `Set` mapeia alguns erros de caminho para `db.ErrTableNotFound`. Depois de `MkdirAll`:

- Falhas de `MkdirAll` (permissão, disco cheio) devem propagar-se como **`fmt.Errorf("...: %w", err)`** com contexto (“ensure key dir”, “ensure sk dir”).
- Se se mantiver `writeBlockedByMissingPath` após `MkdirAll`, só cobre casos patológicos (`ENOTDIR` por ficheiro homónimo de segmento esperado como directório).

### Riscos / limitações (documentar no código ao implementar)

1. **`MkdirAll` não valida schema de negócio** — permite `Set` com `table` arbitrária a criar árvores no disco mesmo sem `CreateTable` lógico; isso pode **adiar** falhas para camadas API. Aceitável como “defesa em profundidade” **junto com** invariantes mais altos (`Adapter`, auth). Opcionalmente, no futuro, exigir `CreateTable` **e** mesmo assim manter `MkdirAll`.

2. **Tabelas “órfãs” no disco** — semelhante ao ponto 1; operação deve ser idempotente e não remover diretórios vazios (fora do âmbito).

3. **Não corrige desalinhamento de `LastSeq`** — apenas evita falha mecânica quando o erro era ausência de directório; replicação/desync continua dependente de **B**.

### Testes recomendados para E

- `Set` sobre tabela cuja entrada em `TbMutex` existe mas `CreateTable` **não** foi chamado: deve **persistir** ficheiros após mudança, sem `ENOENT` no primeiro `WriteFile`.
- Caso **`ENOTDIR`**: criar um ficheiro sólido no caminho onde deveria existir `.../table/key` antes de `Set`; esperar erro claro (não silenciar com `MkdirAll` bem-sucedido — `MkdirAll` falha neste cenário típico dependendo SO; garantir comportamento definido nos testes suportados no CI).

### Critérios de aceite específicos de E

- Não regressão em testes existentes `internal/fsdb` / `internal/db`; novos casos cobrem pelo menos PK simples + SK quando aplicável.

---

## Combinação B + E na entrega

- **B** endereça a **causa sistemica** quando `DeferWrites=true`: `LastSeq` deixa de adiantar-se ao estado real do `inner` entre WAL e disco.
- **E** cobre falhas **`ENOENT`** remanescentes por ordens de chamada inesperadas, concorrência de `TbMutex` sem dirs, ou caminhos de teste/replicação.

Ordem sugerida de implementação nos PRs:

1. **B** primeiro (corrige semântica de checkpoint sob carga).
2. **E** a seguir (reduz ruído operacional e classes de erro de filesystem).

---

## Referências cruzadas

- Problema: [refining/consistence_checkpoint_wal_and_fsdb.md](../refining/consistence_checkpoint_wal_and_fsdb.md)
- Recuperação: [specs/checkpoint-wal-recovery.md](./checkpoint-wal-recovery.md)
- Batch `fsdb`: [specs/reducao-pressao-filesystem-batch-merge.md](./reducao-pressao-filesystem-batch-merge.md)
- Código de integração típico: `internal/wal/wal.go`, `internal/wal/options.go`, `cmd/node/main.go`, `internal/fsdb/write_batcher.go`, `internal/db/adapter.go`, `internal/fsdb/base.go`
