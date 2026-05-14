# Spec: leituras strong vs stale no node (`cmd/node/main.go`)

## 1. Objetivo

Permitir dois modos de leitura sobre o estado replicado pelo Raft, de forma que **carga analítica e leituras tolerantes a atraso não concentrem no líder**, enquanto **quem precisa de consistência forte continue obtendo linearizabilidade via líder**.

| Modo | Semântica alvo | Onde executar |
|------|----------------|---------------|
| **Strong read** | Linearizável em relação às escritas commitadas pelo cluster | Preferencialmente **líder** (após barreira adequada — ver §4) |
| **Stale read** | Eventual / “best effort”: pode devolver dados atrasados em relação ao último commit global | **Qualquer réplica** (follower ou líder), lendo o estado já aplicado localmente |

**Escopo deste doc:** decisões de produto/API e alternativas de implementação no binário servido por `cmd/node/main.go` e nas camadas `internal/` (Raft + futura máquina de estados). Não prescreve um único patch; compara caminhos por **simplicidade vs correção forte**.

---

## 2. Estado atual do projeto (referência)

- O `cmd/node/main.go` sobe gRPC (Raft entre pares) e HTTP com `POST /cmd/propose` e `GET /state`.
- O handler de proposta só aceita comandos no **líder**; existe **TODO** para retornar identificação/endereço do líder ao cliente.
- Entradas commitadas são expostas via `Applied() <-chan`; a integração completa com um KVS durável (`internal/store`, etc.) no loop do node ainda é evolução natural.
- Esta spec assume que, no futuro, **cada processo node** mantém uma **réplica local** do estado derivada do log commitado (mesma ordem de aplicação em todos os que estão ao dia com o log).

Tudo que segue pressupõe essa máquina de estados **determinística** e alimentada apenas por entradas Raft commitadas na ordem do log.

---

## 3. Definições precisas

### 3.1 Strong read (linearizável)

Uma leitura é **linearizável** se o resultado “parece” ter ocorrido em um único ponto na linha do tempo global, **compatível com a ordem real** das escritas já observadas pelo cliente e com todas as escritas commitadas antes desse ponto.

No Raft, isso **não** é garantido apenas por “ler no líder o valor em memória”: em falhas de rede/partição, um ex-líder pode ainda aceitar leituras por um tempo se não houver verificação de liderança e barreira com o índice commitado.

Portanto, “strong” neste doc significa **linearizável de verdade**, não só “pedido foi ao líder”.

### 3.2 Stale read (eventual)

Leitura sobre o estado local **`lastApplied`** (ou equivalente), **sem** esperar alinhamento com o commit mais recente do cluster. Followers tipicamente têm `lastApplied ≤ commitIndex_global` sob normalidade; podem ficar várias entradas atrás sob carga ou partições.

Semântica útil para: dashboards, recomendações, pré-visualizações, agregações onde “quase atualizado” basta.

---

## 4. Abordagens para strong read (do mais simples ao mais rigoroso)

### A — “Só líder, sem Read Index” (muito simples, **linearização frágil**)

**Ideia:** Aceitar strong read apenas no líder; ler o estado após aplicar até `commitIndex` local.

**Mecânica:**

1. Se `state != Leader` → `409`/`503` + corpo com `leader_id` / URL (complementa o TODO já previsto em `Propose`).
2. Se líder → retornar valor da MV a partir da projeção local.

**Problema:** Se o líder perde mandato e ainda responde leituras, clientes podem ver **estado “split-brain”** ou leituras inconsistentes com escritas recentes. Para protótipo interno pode ser aceitável; para produção não.

**Complexidade:** baixíssima. **Correção strong:** insuficiente para a definição de §3.1.

---

### B — Read Index (Hashicorp Raft / etcd-style) (**recomendada para strong de produção**)

**Ideia:** Antes de ler, o líder **fixa um índice de leitura** `readIndex` igual ao `commitIndex` atual (ou protocolo equivalente), **confirma que ainda é líder no termo atual** (via heartbeat/quorum ou round-trip dedicado), e só então serve a leitura quando a máquina local aplicou até pelo menos esse índice.

**Propriedade:** Leituras ordenadas com as escritas commitadas **sem** precisar registrar cada GET no log Raft.

**Passos típicos no líder:**

1. Incrementar/registrar um “read state” e mandar `Heartbeat`/`AppendEntries` vazio (ou RPC `VerifyLeader`) aos followers.
2. Quando quorum confirmar o termo atual → “linha de leitura” válida.
3. Esperar `lastApplied >= readIndex` (localmente já garantido após commit + apply ordenado).
4. Ler estado da MV.

**Em follower para strong:** Em geral **não**: follower linearizável normalmente precisa consultar o líder para obter um `readIndex` e então **aguardar aplicar até esse índice** — volta a haver trabalho no líder para o fence, mas **o payload da leitura** pode ser servido no follower depois da barreira (variação “processed read index on follower”) — mais complexidade de API interna.

**Complexidade:** média (novos RPCs ou uso explícito do fluxo de replicação para “confirm leadership”). **Correção:** alinha com linearização usual em sistemas Raft maduros.

---

### C — Registrar leitura como comando no log (**simples conceitualmente, caro em throughput**)

**Ideia:** Strong read = `Propose("READ key …")`; commit total ordena read entre writes.

**Prós:** Linearização “grátis” pela ordem do log.  
**Contras:** Amplifica write path e latência; líder faz mais trabalho por GET — **vai contra** o objetivo de não sobrecarregar o líder para strong reads frequentes.

**Complexidade:** baixa em código se já existe `Propose`. **Escalabilidade:** ruim.

---

### D — Lease de liderança para reads (**simples só no papel**)

**Ideia:** Líder assume válido um lease de alguns ms sem contactar quorum antes de cada read.

**Contras:** Depende de relógios ou bounds de tempo; fácil errar sob GC pause, VM steal time, ou drift. Em Go distribuído é **desaconselhado** como primeira escolha frente a Read Index.

---

## 5. Abordagens para stale read

### E — Leitura local direta (**mais simples**)

**Ideia:** Qualquer papel (follower ou líder) expõe `GET …` com modo stale; handler lê MV **sem** RPC ao líder.

**Detalhes úteis:**

- Opcionalmente incluir na resposta metadados: `applied_index`, `commit_index_local`, `term`, `role` — clientes ou proxies podem observar lag.
- Sob indisponibilidade do líder, followers continuam servindo stale se o estado local existir — ótimo para disponibilidade de leitura.

**Complexidade:** baixa após existir MV local em cada node.  
**Risco:** cliente pode não perceber **quão** stale está sem metadados ou política de `max_staleness` (extensão futura).

---

### F — Stale com limite de atraso (**bounded staleness**)

**Ideia:** Rejeitar ou esperar se `commitIndex - lastApplied > K` ou se `lag_ms > T`.

**Complexidade:** média (métricas de tempo coerentes entre réplicas são difíceis sem clocks sincronizados; usar índices é mais limpo).

---

## 6. Onde escolher o modo (contrato HTTP sugerido)

Opções equivalentes em espírito (escolher uma para consistência da API):

1. **Query:** `GET /v1/...?consistency=strong|stale`
2. **Header:** `Read-Consistency: strong | stale`
3. **Rotas:** `GET /strong/...` vs `GET /stale/...` — explícito mas proliferates handlers.

Para strong em não-líder:

- **`307 Temporary Redirect`** para URL base do líder (precisa descobrir líder — ver §7), ou
- **`409 Conflict`** / **`503`** com JSON `{ "leader_http": "...", "leader_id": "..." }` (adequado para clientes que não seguem redirects).

---

## 7. Descoberta do líder e mudanças em `cmd/node/main.go`

Hoje os peers são endereços gRPC; o HTTP pode estar em outra porta (`-http-addr`). A spec recomenda:

1. **Manter um mapa `node_id → http_base_url`** (config estática inicialmente: flags ou ficheiro), suficiente para redirects e para clientes externos.
2. Completar o TODO em `Propose`: respostas de erro devem incluir **quem é o líder** e **como contactá-lo em HTTP**.
3. Opcional: endpoint `GET /cluster` com vista eventual dos membros e papel atual — ajuda operators; não substitui strong read.

O `main.go` apenas **compõe** handlers e flags; a lógica forte/stale deve morar em `internal/api` + `internal/raft` (Read Index) + camada de estado, mantendo interfaces pequenas no consumidor (`internal/` guidance do projeto).

---

## 8. Comparação objetiva — “o mais simples que atinge o objetivo”

| Abordagem | Esforço | Strong correto | Tira carga do líder nas leituras “baratas” |
|-----------|---------|----------------|--------------------------------------------|
| A — Só líder, read direto | Mínimo | Não | Não |
| B — Read Index no líder | Médio | Sim | Stale em E redireciona carga para followers |
| C — Read via log | Baixo | Sim | Não (pior) |
| E — Stale local | Baixo (pós-MV) | N/A | Sim |

**Conclusão:** O caminho **mais simples que honra “strong = linearizável” e “stale = followers”** é combinar:

- **(E)** leitura local em qualquer node com flag/rota/header `stale`;
- **(B)** strong read apenas no líder com **Read Index** (ou um subset documentado de B se quiserem entregar em fases).

Fase **MVP interna** possível: **A + E** com aviso explícito na documentação de que strong ainda não é linearizável sob eleição de líder — útil para desenvolvimento, desde que não se rotule como produção.

---

## 9. Ordem de implementação sugerida

1. Aplicar comandos commitados a uma MV partilhada por todo o processo (se ainda não estiver no caminho HTTP).
2. Expor stale read na HTTP API em todos os papéis + metadados de lag opcionais.
3. Melhorar erros de `Propose` e futuros handlers com **hint do líder** (+ config HTTP dos peers).
4. Implementar Read Index (ou integrar biblioteca Raft que já o suporte) para strong read no líder.
5. Testes: cenários de failover durante read; propriedade “read após write” com cliente fixando strong.

---

## 10. Fora de âmbito (mas relacionado)

- **Transactions / snapshot isolation** entre reads strong — nível acima desta spec.
- **Read-only followers** em multi-região com RTT alto — stale (E) é o padrão comercial típico.
- **Linearizável em follower** sem tocar no líder — exige hipóteses extras (leases fortes, clocks) ou replicação estado-leitura especial; deliberadamente não priorizado aqui.

---

## Referências conceituais

- Diego Ongaro, John Ousterhout — *In Search of an Understandable Consensus Algorithm (Raft)* — § para commit e propriedades de líder.
- Etcd / Hashicorp Raft — padrão **Read Index** para reads linearizáveis sem logar cada read.
