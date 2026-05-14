# WAL puro na `Facade` (sem `store.Store`)

Este documento descreve como integrar o log **apenas** via `internal/store/wal.go` (tipo `WAL` e formato `Entry`) na camada `internal/db/facade.go`, **sem** compor ou delegar para `store.Store`. Inclui tradeoffs e o que precisa ser duplicado, extraído ou exposto na API do pacote `store`.

## Contexto atual

- **`store.WAL`** (`wal.go`): append-only com framing (`len || payload || CRC32`), `Append`, `Replay`, `Flush`, `Close`, `RepairTruncatesTail`; opções em `store.Options` (`Durability`, `AfterSync`).
- **`store.Store`** (`store.go`): orquestra diretório, checkpoint em disco, `memdb.DB`, sequência monotónica (`nextSeq`), transações (`OpBegin`…`OpCommit`), `memdb.SetDurable` via `storeDurable`, e usa **`openWAL` não exportado** para abrir o ficheiro.
- **Replay semântico**: `replay_state.go` + `applyEntry`/`decodePutItem` em `comuns.go` aplicam `Entry` ao `memdb`, incluindo agrupamento transacional entre `Begin` e `Commit`.
- **`internal/db/facade.go`**: pretende combinar `memdb`, `fsdb` e WAL; hoje o WAL **não** está ligado de forma completa ao ciclo de vida (open → repair → replay → append).

Objetivo explícito: a **`Facade`** passa a ser o **único agregador** que decide quando gravar no WAL e como recuperar estado após crash, **sem** instanciar `*store.Store`.

---

## O que “WAL puro” cobre vs o que falta na `Facade`

| Responsabilidade | Em `wal.go` sozinho | Na `Facade` (ou código auxiliar) |
|------------------|---------------------|-----------------------------------|
| Formato no disco + truncagem de cauda corrupta | Sim (`RepairTruncatesTail`, `Replay`) | Chamar repair antes de abrir |
| Abrir ficheiro `data.wal` | Apenas via `openWAL` **privado** | Exportar construtor (ex.: `store.OpenWAL(path, opts)`) ou mover tipo para pacote próprio |
| Atribuir `Seq` monotónico | Não | Estado na `Facade` (ou serviço dedicado), alinhado com checkpoint |
| Replay para `memdb` | Não (`Replay` só invoca callback) | Reutilizar lógica tipo `replayState.apply` + `applyEntry`, ou extrair pacote partilhado |
| Checkpoint (`loadCheckpoint` / `saveCheckpoint`) | Não | Mesma política que `Store` ou política nova explícita |
| `memdb.DurableWriter` / transações | Não | Implementações na `Facade` que espelham `putLocked`, `deleteLocked`, `commitTransactionLocked` |

Conclusão: “usar só `wal.go`” resolve **persistência binária**; **toda a política de recuperação e mutação durável** continua a precisar de um dono — aqui, a `Facade` (ou um tipo interno `internal/db`/`internal/durable` por baixo).

---

## Opções de arquitetura

### A — `Facade` importa `store` apenas para WAL + tipos (`Entry`, `Options`, ops)

- Exportar algo como `store.OpenWAL(path string, opts Options) (*WAL, error)` (hoje `openWAL` é privado).
- Copiar ou **extrair** para pacote neutro (`internal/walreplay` ou `internal/store/replay.go` exportado) a função `apply` equivalente a `replayState` + uso de `applyEntry`.

**Prós:** Poucas mudanças no formato em disco; um único `WalFileName`.  
**Contras:** `internal/db` depende de `internal/store`; risco psicológico de “misturar” com `Store` se a equipa usar imports largos.

### B — Extrair `WAL` (+ `Entry`, `Options`) para pacote novo `internal/wal`

- `store` passa a usar esse pacote por baixo (refactor mecânico), ou mantém aliases temporários.

**Prós:** Fronteira clara: “log” ≠ “loja durável completa”.  
**Contras:** Mais ficheiros e PR maior; migração de imports e possíveis duplicações transitórias.

### C — Manter WAL em `store` mas extrair só o “motor de replay”

- Pacote pequeno só com `ReplayToMemDB(db *memdb.DB, cpSeq uint64, apply ...)` encapsulando estado transacional.

**Prós:** `Facade` não duplica semântica de `Begin`/`Commit`.  
**Contras:** Ainda há escolha de onde vive esse pacote (`store` vs neutro).

Recomendação pragmática para este repo: **A ou C** primeiro (menor superfície), com export do construtor do WAL se a `Facade` for o único cliente além do `Store`.

---

## Tradeoffs detalhados

### 1. Duplicação vs extração da lógica de replay

- **Duplicar** `replayState` na `Facade`: rápido, mas dois sitios a corrigir quando `Entry`/ops mudarem.
- **Extrair** replay para função/pacote único: um único comportamento para `Store` e `Facade`; custo único de refactor e revisão de testes.

**Tradeoff:** consistência de longo prazo vs tamanho imediato do diff.

### 2. Checkpoint e `Seq`

O `Store` usa `max(checkpoint.LastSeq, maxSeqDoWAL)` para `nextSeq`. Se a `Facade` ignorar checkpoint, pode **reaplicar** operações já capturadas no snapshot ou **regredir** `Seq` — comportamento incorreto para novos appends.

**Tradeoff:** reimplementar checkpoint na `Facade` (complexidade) vs continuar a depender de helpers já existentes em `store` (acoplamento ao pacote que também contém `Store`).

### 3. Concorrência e ordem WAL ↔ memória

Hoje `Store` usa um `mutex` global para sequências WAL + mutations em memória. Uma `Facade` que apenas **injeta** `WAL` sem modelo claro pode permitir corrida entre goroutines (`nextSeq`, append, apply).

**Tradeoff:** um lock central na `Facade` (simples, igual ao `Store`) vs locks mais finos (maior risco de inconsistência WAL/memória).

### 4. Durabilidade (`Buffered` vs `SyncEveryWrite`)

`wal.go` já implementa ambos; a política de **quando** chamar `Flush()` (ex.: após batch de comandos Raft, ao fechar nó) é decisão da camada que usa a `Facade`.

**Tradeoff:** latência vs segurança em falha de energia; `Buffered` exige disciplina explícita de flush nos limites correctos.

### 5. API pública do pacote `store`

Exportar `OpenWAL` aumenta superfície pública “interna”; alternativa é tipo construtor só em `internal/db` via interface definida no consumidor — mas alguém tem de abrir o ficheiro.

**Tradeoff:** API mínima exportada (`OpenWAL`, `WAL`) vs factoring para pacote `internal/wal` com API estável.

### 6. `fsdb` vs WAL

A `Facade` já tem `fsdb` (metadados/tabelas em disco). WAL e `fsdb` são **canalidades diferentes**: WAL recupera **linhas** em `memdb`; `fsdb` pode definir esquema/directórios. É preciso documentar qual é fonte de verdade para “tabela existe” vs “há dados na memória recuperados pelo WAL”.

**Tradeoff:** ordem de inicialização (criar tabela em `fsdb` antes de aplicar puts do WAL na mesma tabela) para não falhar replay.

### 7. Testes

`store_test.go` cobre WAL + replay + transações através do `Store`. Com WAL na `Facade`, ou bem há **testes de integração** equivalentes na `Facade**, ou regressões silenciosas em `Begin`/`Commit`/checkpoint.

**Tradeoff:** copiar cenários-chave vs factorizar helpers de teste partilhados.

---

## Linha de implementação sugerida (checklist)

1. Exportar construtor do WAL (`OpenWAL`) ou equivalente; garantir `RepairTruncatesTail` antes do open no caminho usado pela `Facade`.
2. Decidir política de checkpoint: reutilizar funções actuais do `store` ou mover para pacote neutro invocado pela `Facade`.
3. Extrair ou partilhar **replay** (`replayState` + `applyEntry`): obrigatório para não divergir da semântica actual.
4. Na `Facade`, manter `nextSeq`/`nextTxID` e métodos que espelham `putLocked` / `deleteLocked` / transaccional, registando `memdb.SetDurable` com um `DurableWriter` que só delega para esses métodos.
5. Garantir **um mutex** (ou modelo documentado) para append+memória.
6. Testes: abrir diretório, mutar via `Facade`, crash simulado (reopen), replay + checkpoint.

---

## Riscos residuais

- **Dois caminhos de durabilidade** (`Store` vs `Facade`+WAL) no mesmo binário aumentam confusão operacional; convém documentar qual entrypoint usa qual.
- **Evolução do formato `Entry`:** qualquer mudança em `entry.go` exige migração ou versionamento; WAL puro não resolve isso por si.
- **Tamanho do WAL** sem checkpoint periódico: crescimento e tempo de startup; política de checkpoint deve acompanhar a `Facade` se for produção.

---

## Resumo

Integrar WAL “puro” na `Facade` é viável com o código actual: **`WAL` + `Entry` + `Replay`** já são o núcleo em `wal.go`. O custo real está em **não** herdar `Store`: é preciso **reproduzir ou partilhar** sequência, checkpoint, replay transaccional e hooks `memdb`, com tradeoffs claros entre duplicação, acoplamento ao pacote `store` e um refactor maior para pacotes `internal/wal` + `internal/walreplay`.
