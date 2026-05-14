# Plano de implementação: persistência em disco

Este documento descreve **fases**, **entregáveis** e **critérios de aceite** para levar o KVS de **apenas memória** para **WAL + recuperação**, alinhado ao [ADR 0001](../adr/0001-formato-serializacao-persistencia-kvs-generico.md) (serialização) e ao [ADR 0002](../adr/0002-estrategia-persistencia-memoria-disco-distribuicao.md) (modelo log + checkpoints + evolução distribuída).

**Estado atual (referência):** `internal/db` mantém `Table` / `VirtualTable` com `Data` e `Fk` em RAM; valores são CBOR via `internal/code`. `internal/fs` está vazio. Não há abrir/fechar base nem recuperação.

---

## Princípios que o plano respeita

1. **Fonte de verdade para crash recovery:** append-only **WAL**; snapshots só aceleram startup.
2. **Codec em disco:** compatível com o ADR 0001 (ex.: CBOR/MessagePack por registo com `version` + `codec` no envelope quando fizer sentido).
3. **Semântica explícita:** modos de durabilidade (sync vs. buffer) **nomeados** na API/config — não misturar garantias no mesmo modo.
4. **Ordenação:** cada entrada do WAL tem **offset/sequência monotónica** para replay determinístico.

---

## Fase 0 — Desenho do registo WAL (especificação curta)

**Objetivo:** fixar o layout binário de uma **entrada** antes de codificar I/O.

**Entregáveis:**

- Pequena secção em código ou neste spec: campos mínimos sugeridos  
  - `magic` / `version` do formato do ficheiro  
  - `entry_seq` (uint64 monotónico)  
  - `op` (`put` | `delete`)  
  - `table` (nome da tabela, se multi-tabela)  
  - `key`, `fk` (strings; `fk` pode ser vazio se não aplicável)  
  - `value` (bytes CBOR do valor, só em `put`)  
  - `checksum` opcional (CRC32/CRC64) por entrada

**Aceite:** exemplo serializado (hex ou teste golden) de uma entrada `put` e uma `delete`; documento diz como tratar **corrupção** (abortar recovery vs. truncar última entrada incompleta).

**Dependências:** ADR 0001 (valor já é CBOR em memória; no WAL pode reutilizar os mesmos bytes de `Encode`).

---

## Fase 1 — Abstracção de armazenamento e abertura de “store”

**Objetivo:** introduzir um **Store** (ou equivalente) que saiba **caminho base**, **criar** estrutura de ficheiros e **ligar** a uma `DB` / tabelas.

**Caminho de armazenamento (runtime):** a árvore de ficheiros do store (WAL, metadados, snapshots futuros) reside sob **`./tmp`** relativamente ao **directório de trabalho actual** do processo (ex.: `Open("./tmp", opts)` ou constante `DefaultDataDir = "tmp"` com resolução `./tmp`). Testes unitários continuam a usar `t.TempDir()` para isolamento; o **executável de exemplo** e o uso “local” documentado assumem `./tmp`.

**Entregáveis:**

- Tipo `Store` (nome final à escolha) em `internal/fs` ou pacote dedicado (`internal/store`), com:
  - `Open(path string, opts Options) (*Store, error)` — com `path` default ou convenção explícita apontando para **`./tmp`**
  - `Close()` (flush pendente conforme política)
- Opções mínimas: `Durability` (`SyncEveryWrite` | `Buffered` + intervalo/tamanho de batch — detalhe na Fase 3).
- Integração mínima com `db.DB`: ou construir `DB` a partir do `Store`, ou anexar um **WriteAheadLogger** às operações mutáveis.
- `tmp/` listado no `.gitignore` do repositório (dados locais não versionados).

**Aceite:** `Open` com caminho `./tmp` (ou equivalente) **cria** o directório se não existir e materializa a estrutura de ficheiros esperada num directório vazio; `Open` num directório já inicializado com WAL existente **não** aplica mutações à RAM até a Fase 2 (pode ser no-op de recovery inicialmente com teste que só verifica abertura).

**Testes:** testes de integração com `t.TempDir()` (caminho arbitrário); opcionalmente um teste que valida `mkdir`/layout usando um `tmp` sob o temp de teste para espelhar `./tmp` sem poluir o cwd.

---

## Fase 2 — WAL append + recovery (nó único)

**Objetivo:** cada `Set` / `Delete` (e sessões que comitam o mesmo) **append** ao WAL; ao **Open**, **replay** reconstrói `VirtualTable` em memória.

**Entregáveis:**

- Componente `WAL` com `Append(entry)` e `Replay(apply func(Entry) error) error`.
- Instrumentar `Table` (ou camada fina à volta): após mutação em RAM bem-sucedida, append; **ordem** deve ser acordada (ver nota abaixo).
- Recovery: ao subir, truncar entradas incompletas se o formato o permitir; depois replay sequencial.

**Nota de ordenação (decisão de implementação):**  
- *Opção A (simples):* append **antes** de aplicar à RAM; em crash após append, replay idempotente (reaplicar mesmo `put`/`delete`).  
- *Opção B:* aplicar RAM primeiro e append depois (maior risco de divergência se crash entre os dois sem cuidado).  
**Recomendação no plano:** Opção A ou “append then apply” com idempotência por `entry_seq`.

**Aceite:** teste que: escreve N chaves, mata processo simulado (re-abrir store), verifica igualdade de conteúdo; teste só `delete` idempotente.

**Testes:** ficheiro WAL corrompido no meio — comportamento documentado e testado (falha clara ou truncagem controlada).

---

## Fase 3 — Política de `fsync` e modo bufferizado

**Objetivo:** cumprir o ADR 0002: modo **forte** (sync configurável por escrita ou grupo) vs. **latência** (buffer + flush periódico ou por tamanho).

**Entregáveis:**

- Implementação de `SyncEveryWrite` (ou grupo “commit” explícito se introduzires transacções no log).
- Modo `Buffered`: `Flush()` periódico + `Close` flush completo; **documentar** janela de perda.
- Opcional: `BatchWindow` / `MaxBatchBytes` na API pública ou só `Options` internas na primeira versão.

**Aceite:** benchmark rudimentar (opcional) ou teste que conta `Sync` num mock `WriteSyncer`; documentação da semântica no README ou neste spec.

---

## Fase 4 — Checkpoint / snapshot

**Objetivo:** reduzir tempo de **replay** após muitas entradas.

**Entregáveis:**

- Formato de **snapshot** (pode serializar `map` `Data`/`Fk` por tabela em CBOR/MessagePack com versão de formato — alinhado ao ADR 0001).
  - Metadados: `last_applied_seq`, timestamp opcional.
- Rotina `Checkpoint()` gravando snapshot + marcação no WAL ou ficheiro índice separado.
- Recovery: carregar último snapshot válido + replay WAL **após** `last_applied_seq`.

**Aceite:** teste com 10k entradas: checkpoint + truncagem ou arquivo de WAL rotacionado (conforme desenho) reduz linha de replay medida em teste (contagem de entradas relidas).

**Nota:** truncagem/compacção do WAL é um sub-tópico; pode ser **Fase 4b** para não bloquear snapshots funcionais.

---

## Fase 5 — Encaixe na API pública e multi-tabela

**Objetivo:** `DB` com várias tabelas persiste nome da tabela em cada entrada WAL; `CreateTable` recupera estado por nome.

**Entregáveis:**

- Todas as mutações incluem **nome da tabela**.
- `main` ou exemplo: abrir store, operações, fechar.

**Aceite:** teste multi-tabela com recovery.

---

## Fase 6 — Preparação para cluster (âmbito limitado)

**Objetivo:** não implementar Raft completo no escopo imediato, mas **deixar extensível**: entradas com `entry_seq` único global, interface de “aplicar comando” que uma futura camada de replicação possa reutilizar; documento de **requisitos** para quorum (referência ADR 0002).

**Entregáveis:**

- Lista de gaps: rede, eleição de líder, log replicado, snapshot membros.
- Opcional: trait/interfacce `Appender` / `Applicator` em Go para isolar WAL de transporte.

**Aceite:** revisão breve deste plano + ADR 0002 assinalando “WAL local estável” como pré-requisito de cluster.

---

## Riscos e dependências

| Risco | Mitigação |
| ----- | --------- |
| Contenção no `Table.mu` com I/O | Writer dedicado + canal de batches para WAL (Fase 3+) |
| Replay lento | Checkpoints (Fase 4) + medir |
| Formato WAL em evolução | `version` no header e migração documentada |
| Sessão `NewSession` vs. WAL | Definir se uma sessão gera **um** commit no log ou N appends (documentar) |

---

## Ordem sugerida de trabalho (checklist)

- [ ] Fase 0: spec do registo WAL + exemplos
- [ ] Fase 1: `Open`/`Close` + layout sob `./tmp` + `.gitignore`
- [ ] Fase 2: append + replay + testes crash/reopen
- [ ] Fase 3: políticas `fsync` / buffer
- [ ] Fase 4: snapshot + recovery híbrido
- [ ] Fase 5: multi-tabela + exemplo
- [ ] Fase 6: notas e interfaces para cluster

---

*Última revisão: 2026-03-30. Actualizar quando o ADR 0002 passar a Aceito ou quando o formato de entrada WAL estiver congelado em código.*
