# Transactional Session (commit/rollback)

## Contexto

`Table.NewSession` combina **exclusão mútua por tabela** com **staging** das mutações em `*Tx` até ao fim de `fn`: só então gravam-se no WAL (BEGIN…COMMIT) ou aplicam em memória. Escritas diretas na tabela (`Table.Set` / `Store.Put` sem sessão) continuam a ser **uma operação por entrada** no WAL.

Objetivo do desenho (e semântica atual da sessão) — **tudo-ou-nada** dentro de `NewSession`:

- **commit**: se `fn(tx)` terminar sem erro (e contexto ok), aplicar todas as mutações.
- **rollback**: se `fn(tx)` falhar em qualquer ponto (ou timeout/cancel), **nenhuma mutação** deve ser persistida/aplicada como efeito da sessão.

Este documento propõe abordagens simplificadas e seus tradeoffs.

## Invariantes desejadas (MVP)

- **Atomicidade**: todas as mutações feitas dentro da sessão são aplicadas ou nenhuma é.
- **Isolamento**: como já existe lock de sessão por tabela, podemos manter isolamento serial por tabela (sem concorrência dentro da mesma tabela).
- **Durabilidade** (quando `store.Open`): após o commit retornar sucesso, as mutações devem sobreviver a restart conforme a política de WAL (ex.: `SyncEveryWrite` vs `Buffered` + `Flush`).
- **Semântica de erro**: qualquer erro em `Set/Delete` dentro da sessão implica rollback (e o erro deve ser retornado).

Não-objetivos iniciais (podem vir depois):

- Transações multi-tabela (atomicidade atravessando várias tabelas).
- Níveis de isolamento além de serial por tabela.
- “Savepoints” (rollback parcial) dentro da sessão.

## Terminologia

- **staging**: armazenar mutações temporariamente (em memória) durante a sessão.
- **apply**: aplicar mutações em memória na `Table`.
- **persist**: registrar mutações no WAL (ou equivalente).

## API

```go
func (t *Table) NewSession(ctx context.Context, fn func(tx *Tx) error) error
```

`Tx` acumula `Set`/`Delete` em ordem; `Get`/`GetByFk` veem o estado da transação (replay da lista sobre a tabela base). Não há segunda API de sessão — `Put`/`Delete` diretos no store são para operações únicas fora deste modelo.

## Abordagem 1 — Staging em memória + commit “apply then persist” (mais simples, mas perigosa)

### Ideia

Durante a sessão, acumular mutações em uma lista (ou map). No final:

1. Aplicar tudo em memória (na `Table`).
2. Persistir tudo no WAL.

### Implementação simplificada

- **Tx** guarda um `[]mutation` (puts/dels).
- `Tx.Set/Delete` apenas valida/serializa e adiciona na lista (não chama `Table.Set/Delete`).
- `Tx.Get` pode:
  - ler da tabela e “overlay” mutações pendentes (mais trabalho); ou
  - ser restrito (MVP: `Get` lê só estado base) — geralmente não é aceitável para read/modify/write.
- No commit:
  - adquirir `t.mu` uma vez e aplicar mutações na memória (usando `addLocked/deleteLocked`).
  - depois, gravar no WAL com múltiplos `Append`.

### Tradeoffs

- **Pró**: fácil de implementar; não exige mudar formato do WAL.
- **Contra (crítico)**: se você aplicar em memória e **falhar na persistência** no meio, o processo pode ficar com estado em memória diferente do que está durável; após restart, replay pode não refletir o que ocorreu em memória.
- **Contra**: para ser consistente, teria de persistir antes de aplicar — o que leva à Abordagem 2/3.

**Recomendação**: não usar esta abordagem se durabilidade/WAL estiver ativa.

## Abordagem 2 — Staging em memória + commit “persist then apply” (boa base; ainda não é atomicidade durável sem WAL transacional)

### Ideia

Staging em memória durante a sessão. No commit:

1. Persistir todas as mutações no WAL.
2. Se persistência completa OK, aplicar tudo em memória.

Se falhar durante a persistência, nada foi aplicado em memória e a sessão retorna erro (rollback efetivo em memória). Porém, pode existir **persistência parcial** no WAL se a falha ocorrer no meio dos appends.

### Implementação simplificada

- Idem staging do Tx.
- No commit, com lock do `Store` (`s.mu`) segurado:
  - gerar N entradas de WAL e chamar `wal.Append` N vezes.
  - só depois chamar `ApplyPut/ApplyDelete` N vezes.

### Tradeoffs

- **Pró**: evita “aplicou em memória mas não persistiu”.
- **Contra**: **não garante atomicidade durável**: se 2 de 5 appends forem gravados e o 3º falhar, o WAL fica parcialmente escrito; após restart, o replay aplicará as 2 primeiras mutações.
- **Contra**: para “rollback” real, seria necessário compensar o WAL (escrever entradas de compensação) ou tornar o WAL transacional (Abordagem 3/4).

Quando serve:

- Quando “rollback” significa **não aplicar em memória na mesma execução**, aceitando que o WAL possa ter efeito parcial (geralmente *não* é o que se quer).

## Abordagem 3 — WAL transacional com `BEGIN/COMMIT` (recomendado: atomicidade durável sem muito custo)

### Ideia

Adicionar ao WAL a noção de transação (por tabela, inicialmente):

- `BEGIN(txid)`
- `PUT/DEL(..., txid)`
- `COMMIT(txid)`

No replay:

- aplicar mutações **somente** de transações que tenham `COMMIT` válido.
- mutações sem commit (crash, erro no meio) são ignoradas.

Rollback em runtime:

- se `fn` falhar, não escreve `COMMIT` e descarta staging; nada será aplicado no replay futuro.

### Implementação simplificada (passos)

1. **Modelo**:
   - Introduzir `txid` (ex.: `uint64` ou UUID curto) e novos ops no WAL (`OpBegin`, `OpCommit`).
   - (Opcional) `OpAbort` apenas para debug/observabilidade; não é necessário para correção se replay usa commit.
2. **Writer**:
   - No início do commit: `Append(BEGIN)`.
   - Appendar todas as mutações com `txid`.
   - Appendar `COMMIT`.
   - Dependendo de durabilidade desejada, `Flush/Sync` no final do commit (ou manter a política existente).
3. **Replay**:
   - Fazer uma passada que reconheça transações: acumular mutações por `txid` até ver `COMMIT`.
   - Ao ver `COMMIT`, aplicar todas as mutações acumuladas daquela transação, em ordem.
   - Limpar buffers para txs incompletas ao final (ignoradas).
4. **Integração com `Tx`**:
   - Durante `fn`, staging em memória no `Tx`.
   - Ao final, commit escreve `BEGIN + muts + COMMIT` e aplica em memória.

### Tradeoffs

- **Pró**: atomicidade durável verdadeira no modelo “WAL-only” (sem precisar reverter WAL).
- **Pró**: rollback é barato (não escreve commit).
- **Pró**: tolera crash no meio do commit (tx incompleta é ignorada).
- **Contra**: muda o formato do WAL (necessário versionamento/backward compatibility).
- **Contra**: replay precisa gerenciar buffer de mutações por transação (memória proporcional ao tamanho de uma transação em aberto).
- **Contra**: precisa decidir escopo:
  - por tabela (mais simples: `Table.NewSession` grava apenas em uma tabela)
  - multi-tabela (mais complexo: exigiria um lock global ou protocolo mais elaborado)

Notas de durabilidade:

- Se a política for `Buffered`, o commit só é “durável” quando o buffer for flushado. Para semântica “commit durável”, o commit deveria chamar `Flush()` (ou oferecer opção).

## Abordagem 4 — WAL com “batch append atômico” (bom, mas depende de IO/FS; mais complexo)

### Ideia

Garantir que um conjunto de mutações seja gravado como **um único append lógico** no WAL, de modo que ou:

- o batch inteiro aparece íntegro, ou
- não aparece (ou é detectado como truncado/corrupt e descartado).

Isso pode ser feito encapsulando um “batch record” com comprimento + CRC (sem precisar `BEGIN/COMMIT`), por exemplo:

- `BATCH{n, [entries...]}` com CRC do payload total.

No replay:

- cada batch é validado por CRC; se o batch não estiver completo, truncar/ignorar.

### Implementação simplificada

- Novo tipo de registro no WAL: `OpBatch` cujo payload contém várias operações.
- `AppendBatch([]Entry)` que serializa tudo e grava uma vez.
- `Store` expõe `Put/Delete` unitário como hoje, mas o commit transacional usa `AppendBatch`.

### Tradeoffs

- **Pró**: replay simples; atomicidade por batch vem do framing+CRC.
- **Pró**: menos overhead de múltiplos `Append`.
- **Contra**: exige alterar o formato do WAL (compat).
- **Contra**: para batches grandes, o frame grande aumenta alocação e custo de cópia.
- **Contra**: ainda precisa decidir quando aplicar em memória (em geral, “persist batch then apply”).

## Abordagem 5 — Copy-on-write (COW) da tabela durante a sessão (boa ergonomia de leitura; custo de memória)

### Ideia

Criar uma visão isolada da tabela durante a sessão:

- `Tx` opera sobre uma estrutura COW (ou clone) de `Data/Fk`.
- `Get`/`GetByFk` funcionam naturalmente nesse snapshot.
- No commit: calcular “diff” (mutações) entre snapshot original e o estado final do Tx e persistir/aplicar.

### Implementação simplificada

- Ao iniciar Tx: clonar `Data` e `Fk` (ou manter base e overlay).
- Operações do Tx mexem só no snapshot.
- No commit:
  - produzir lista de puts/dels (diff).
  - usar Abordagem 3 (BEGIN/COMMIT) ou 4 (BATCH) para persistência atômica.
  - aplicar diff em memória (ou simplesmente substituir mapas se estiver sob lock exclusivo e aceitar custo).

### Tradeoffs

- **Pró**: API muito agradável para read/modify/write; `Get` sempre vê alterações “dentro da transação”.
- **Pró**: simplifica regras de leitura e FK.
- **Contra**: custo de memória e CPU proporcional ao tamanho da tabela (clonar mapas grandes é caro).
- **Contra**: diff pode ser caro (ou complexo se `Value` grande/encode).

## Recomendação prática (ordem de implementação)

### MVP recomendado

- **Escopo**: transação por tabela (compatível com o lock atual).
- **Persistência**: **Abordagem 3 (BEGIN/COMMIT)**, pois dá rollback durável correto e crash-safety.
- **Leituras no Tx**:
  - começar com staging por lista de mutações + overlay simples para `Get(key)` (checar del/put pendente antes de consultar base).
  - `GetByFk` pode ser inicialmente “best effort” (overlay) ou documentado como limitação do MVP; idealmente, implementar overlay completo para consistência.

### Evolução

- Se performance de replay/append virar problema: considerar **Abordagem 4 (batch)**, possivelmente combinada com BEGIN/COMMIT (ou substituindo).
- Se ergonomia de leitura for prioridade e tabelas forem pequenas: considerar **Abordagem 5 (COW)**.

## Considerações de compatibilidade

- O payload do WAL usa um único `formatVersion` atual (ver implementação em `internal/store/entry.go`). Ficheiros antigos com versão diferente deixam de ser lidos — migrar apagando o WAL ou via checkpoint + novo log, conforme política do projeto.

## Cenários de falha (o que validar em testes)

- **Erro no meio do `fn`**: nada persistido/aplicado.
- **Erro ao appendar mutações**:
  - Abordagem 3: sem `COMMIT`, replay ignora.
  - Abordagem 4: batch incompleto é truncado/ignorado por CRC.
- **Crash entre `COMMIT` e apply em memória**:
  - Após restart, replay com commit aplicará; em runtime anterior pode não ter aplicado, mas isso é aceitável pois o processo caiu.
- **Timeout/cancel do ctx**:
  - não escrever `COMMIT`; retornar erro.

## Implementação (estado atual)

### API

- [`Table.NewSession`](../../internal/db/session.go): lock por tabela + staging em [`Tx`](../../internal/db/session.go); no sucesso chama [`Store.CommitTransaction`](../../internal/store/store.go) quando o `DurableWriter` também implementa [`TransactionCommitter`](../../internal/db/durable.go) (o `Store` após `store.Open`). Sem store durável, aplica só em memória.
- Leituras: `Get` percorre a lista ordenada de mutações **do fim para o início** e devolve o último `Put` ou `Del` que afeta a chave; se não houver, lê a tabela base. `GetByFk` reúne chaves candidatas (índice `Fk` + puts na transação com esse `fk`) e usa `Get` por chave.

### WAL (`internal/store/entry.go`)

- Payload único (p.ex. `formatVersion` 2): `OpPut` / `OpDel` imediatos, mais `OpBegin` / `OpCommit` para grupos transacionais; `ValueBytes` em Begin/Commit é **txid** em 8 bytes big-endian.
- Sequência por transação commitada: `BEGIN` → um ou mais `PUT`/`DEL` → `COMMIT`.
- Replay ([`replayState`](../../internal/store/store.go)): entre Begin e Commit as mutações ficam em buffer; só após Commit são aplicadas; grupo sem Commit (fim de ficheiro ou transação incompleta) é **ignorado**.
- `CommitTransaction` com `Durability: Buffered` faz **`Flush`** ao final do commit.

### Invariante de lock (ADR 0003)

- O semáforo de sessão é libertado só **depois** de `fn` e de `commitTransaction` (ou erro), de modo que commit/rollback não intercala com outra sessão na mesma tabela.

