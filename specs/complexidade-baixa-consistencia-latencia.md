# Spec (estudo): complexidade inicial baixa com consistência alta e latência baixa (**WAL + `fsdb` apenas**)

## 0. Origem e relação com outras especificações

- **Motivação textual:** refinamento descrito em `refining/reduction_of_complexity.md` (prioridade a consistência + latência em cenários particionados; simplicidade de KVS pequeno/médio).
- **Decisão de armazenamento local (este doc):** **não há `memdb`**. O estado materializado persistido/consultado no path principal é apenas **WAL + `fsdb`**, aplicado deterministicamente depois das entradas commitadas pelo Raft. A antiga tensão **`fsdb` vs `memdb`** fica assim **eliminada** — uma única fonte de verdade durável por réplica para o KV.
- **Complementos no repositório:** decisões já exploradas para leituras fortes vs eventualmente inconsistentes — ver `specs/leituras-strong-vs-stale-node.md`.
- **Convenções de implementação (Go):** módulo `github.com/IsaacDSC/kvs`; código novo de aplicação em `internal/`; interfaces pequenas; `context.Context` em I/O; erros com contexto (`fmt.Errorf("…: %w", err)`). Este documento **não** substitui essas convenções — orienta trade-offs sem introduzir um segundo nível writable em RAM para o mesmo conjunto chave-valor que o disco.

---

## 1. Objetivo deste documento

Consolidar **abordagens** para, desde o design:

1. Manter **complexidade operacional e de código** controlada (“começar simples”), com **uma** pilha persistente bem definida.
2. Preservar **consistência alta** alinhada à premissa do produto (*consistência e latência > disponibilidade sob partição*).
3. Aspiração explícita a **latência baixa** nas operações típicas (KVS não analítico, busca só por chave primária/secundária no futuro, dataset pequeno/médio), explorando **`fsdb`**, batching e cache do SO — **sem** replicação paralela da mesma chave em estruturas em RAM separadas do commit.

**Fora do escopo imediato:** definir formato final de quorum configurável ou lock pessimista/ACID; apenas assinaliar onde estas escolhas **aumentam** ou **mantêm** a complexidade. **Um cache em-processo opcional só volta a ser matéria de spec própria** se no futuro houver medidas que demonstrem SLO não cumpríveis apenas com WAL+`fsdb` + disco/OS.

---

## 2. Triângulo de tensões (mental model)

Todo sistema distribuído com réplicação força escolhas na intersecção de três vetores:

| Vetor | O que preserva… | Como costuma “custar” no design |
|--------|------------------|------------------------------------|
| **Simplicidade** | Menos invariantes paralelos, menos estados divergentes, menos fusões | Latência sob pico depende mais de disco e batching bem feitos que de um cache duplicativo |
| **Consistência forte** | Uma história ordenada compatível com o log Raft | Mais barreiras (ex.: líder/read index); menos paralelismo lógico em leituras fortes |
| **Latência** | UX e batch pequenos | Caminhos assíncronos “rápidos” fora da ordem do log tendem a reintroduzir bugs de coerência; com **uma** verdade (`fsdb` após aplicar Raft), esse risco cai drasticamente |

A premissa declarada (*consistência + latência > disponibilidade em partição*) **não elimina** trabalho para manter reads rápidos: concentra o esforço em **estruturas de dados em disco eficientes**, **menos syscall por batch** (`specs/reducao-pressao-filesystem-batch-merge.md` quando aplicável), e semânticas de leitura **explícitas** (forte vs stale no cluster).

---

## 3. Princípios com WAL + `fsdb` apenas

### 3.1 Uma verdade ordenada (“single sequencing”)

- A **única narração de ordem** das mutações deve ser o **log commitado** (Raft) e a máquina de estados deve ser **determinística** ao aplicá-lo.
- A **persistência observable** dessa máquina, para o modelo KV deste projeto, é **somente WAL + `fsdb`** (replay/recovery já cobertos nos docs de WAL/checkpoint quando existentes). Extensões futuras (por exemplo índices secundários persistidos **no mesmo** `apply`) não contam como segunda “pilha paralela”: integram o modelo durável já ordenado pelo log, não um `memdb` separado.

### 3.2 Persistência apenas após commit — um único `apply(entry)`

Invariante útil para reduzir complexidade:

```text
Durante apply de uma entrada commitada: atualização do estado durável só via o caminho
WAL/fsdb já integrado ao node; não existe segundo writer em RAM paralelo ao mesmo modelo de dados KV.
```

Isto mantém inconsistências tipo “li numa vista e escrevi noutra” **fora da arquitetura alvo**.

### 3.3 Menos estratégias de leitura, mais semânticas explícitas

- Manter paralelizado **tipos diferentes** de leitura (`strong` vs `stale`), não “várias cópias do estado que fingem forte” — ver `specs/leituras-strong-vs-stale-node.md`.
- Evitar REST que sugira forte sem barreira lá onde ainda for obrigatório.

### 3.4 Medições sob WAL + `fsdb` só

Instrumentar sempre que se alterar persistência ou I/O:

- P50/P99 de **proposal → aplicado** e latência no **handler de leitura** forte.
- Pressão de I/O nos caminhos de batch/merge quando existentes (`specs/reducao-pressao-filesystem-batch-merge.md`).

Serve para validar SLIs e decisões futuras opcionais (novo tipo de índice, etc.), **não** para reabrir uma `memdb` sem processo deliberado em nova spec.

---

## 4. Abordagens estruturais (arquitetura alvo vs evoluções no cluster)

### A — **Arquitetura alvo:** estado durável único (**WAL + `fsdb`**)

**Ideia:** Toda atualização observable do KV sob **consistência forte** relativamente ao que foi aplicado do log passa apenas por **persistência combinada WAL + `fsdb`**.

- **Pros:** mínima superfície entre “valor retornado ao cliente forte” vs “valor em disco após aplicar Raft”; modelo mental simples (“Raft ordena → `fsdb` materializa”).
- **Contras:** leituras muito repetidas dependem mais de page cache/OS e eficiência de `fsdb` do que de um mapa sempre quente só em RAM para o KV principal.

**Adequado quando:** dataset pequeno/médio tipo config/metadata e premissa de já preferir correção sob partição a disponibilidade.

---

### B — Leituras fortes apenas no líder com barreira; seguidores para modalidade eventual (**latência no cluster**) 

**Ideia:** Alinhar com `specs/leituras-strong-vs-stale-node.md`: dispersar trabalho distribuindo leituras tolerantes ao atraso nas réplicas, mantendo forte no líder (Read Index onde a rigor técnico o exija).

- **Pros:** menor carga média no líder sem duplicar o armazém local em **`memdb`**.
- **Contras:** complexidade Raft + API de modo de leitura.

(Esta secção já **não** propõe B como uma “segunda cópia do KV”; é só **topologia/cluster**, não segunda pilha.)

---

### C — Futuro: quorum configurável em escrita (**mais poder, mais invariantes**)

Útil apenas quando a spec de produto exigir granularidade diferente das escritas típicas; cada quorum novo multiplica cenários em testes (partição, reconfiguração, eleição).

Recomenda-se **firmar bem** WAL+`fsdb` + modos de leitura antes de acumular essa segunda dimensão de complexidade simultânea.

---

## 5. Consolidar a decisão (antes levantamento em `reduction_of_complexity.md`)

### 5.1 Por que **não** WAL + fsdb + `memdb`

| Problema com `memdb` separado como caminho paralelo ao mesmo modelo KV | Este projeto |
|-----------------------------------------------------------------------|---------------|
| Dois atualizadores (ou dois momentos inconsistentes antes de reconcile) aumentam código e falhas mesmo com boa intenção | Evitável removendo `memdb`; apply único alimenta só `fsdb` |
| Leituras “fortes” exigindo acordo entre dois stores | Desnecessárias quando só existe persistência bem definida |
| Evição/LRU/memória máxima + testes extras | Custos só justificáveis se métricas mostrarem SLO inexequível apenas com WAL+`fsdb` |

### 5.2 Efeitos práticos de ficar apenas com WAL + `fsdb`

| Aspeto | Comentário |
|--------|------------|
| **Consistência** | Uma materialização ordenada pela aplicação do log; menor ambiguidade client-side. |
| **Latência quente** | Depende das propriedades de `fsdb` + SO; workloads podem estar naturalmente rápidos num payload pequeno. |
| **Remoção de código/tests** | Retirar referências ao `memdb` em apply, handlers e testes diminui espaço da matriz regressiva ligada ao dual-store. |

---

## 6. Plano incremental sugerido (alinhado à remoção do `memdb`)

1. **Identificar todas as atualizações e leituras de estado KV** ligadas ao `memdb` e redesenhar para ler/escrever **só** via WAL+`fsdb` no ciclo aplicado pelo Raft (ou inicialização/recovery já existente).
2. **Eliminar branching** REST/gRPC onde se escolhia vista em memória vs disco para o mesmo dado forte.
3. **Regressões:** reinício, replay de log, checkpoints (`specs/checkpoint-wal-recovery.md` e afins onde existentes) garantem mesmo estado só com WAL+`fsdb`.
4. **API:** manter forte vs stale segundo `specs/leituras-strong-vs-stale-node.md` — staleness refere-se ao **lag de replicação Raft**, não a “estar só em RAM”.
5. **Medir:** após a remoção, confirmar P99 de leitura/escrita; documentar resultado para futura discussão apenas se aparecer bottleneck real incapaz de mitigações em `fsdb`/batch/OS.

---

## 7. Critérios de aceite derivados

- [ ] Não há caminho público nem interno de negócio que mantenha um mapa KV em RAM paralelo persistente aos mesmos comandos Raft que atualizam `fsdb`.
- [ ] Todas mutações públicas aparecem no log ou são rejeitadas com erro claro **antes** de alterar apenas um dos armazéns persistidos (`fsdb` é o único alvo KV materializado aqui para além da tape do WAL).
- [ ] Para leitura **strong**, o valor corresponde ao que um replay determinístico do log commitado até o índice de barreira exigiria só com WAL+`fsdb`.
- [ ] Histogramas Prometheus (ou equivalente) cobrem proposal→apply e latências de reads fortes suficientemente para caracterizar regressões quando se mexe em batching ou formato `fsdb`.

---

## 8. Lacunas conscientes / pedidos futuros ao produto ou à spec de armazenamento

- Critérios numéricos explícitos (“P99 alvo”) para ler na API HTTP — dependem da carga e hardware.
- Checkpoint vs replay completo já parcialmente em specs WAL/recovery — alinhar sempre que mudar formato `fsdb` ou flush.
- Índices secundários persistidos devem atualizar pelo **mesmo** `apply` que o KV base; qualquer novo documento deve evitar reintroduzir “KV em RAM só” tacitamente.
- Lock pessimista/ACID será documento próprio porque introduz granularidade transacional (**aumentará** obrigatoriamente a complexidade frente aos comandos atuais).

---

## 9. Resumo executivo

| Pretensão | Abordagem definida aqui |
|-----------|--------------------------|
| **Consistência alta** | Raft ordena; **só WAL + `fsdb`** materializa o modelo KV forte na réplica |
| **Latência baixa** | Otimização em batching/fsdb/page cache/OS; dispersão forte/stale ao nível cluster sem segundo armazém em RAM paralelo ao KV |
| **Baixa complexidade** | **`memdb` removido** como conceito mantido pelo projeto; quorum configurável só depois da base estável |

Este ficheiro é **estudo e critérios**; decisões pontuais de patch devem continuar ligadas aos docs de WAL, checkpoint e leituras fortes já presentes sob `specs/`.
