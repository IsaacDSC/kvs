# Spec: configuração de quorum (Raft/replicação)

## 1. Objetivo e origem

Documentar como evoluir o projeto (`github.com/IsaacDSC/kvs`, Go 1.26.x, pacotes principais em `internal/`) para suportar **configuração do “nível de quorum”** usado quando o Raft decide progresso — em linha com a discussão em `refining/quorum_with_quorum.md`:

| Abordagem | Resumo |
|-----------|--------|
| **A — Config global por variável de ambiente** | Menor flexibilidade, implementação mais simples. |
| **B — Config via comando replicado** | Maior flexibilidade (e possível evolução para “vários quorum” ao longo do tempo), maior complexidade. |

Este ficheiro **não obriga um patch concreto**; compara desenhos, pontos de integração no código atual, riscos e esboços em Go.

**Não-objetivos (nesta spec):** mudança de algoritmo para consenso fora Raft; memberships dinâmicos com joint consensus; quorum geométrico estilo Dynamo sem modelo formal.

---

## 2. Estado atual (referência no repositório)

Em resumo:

1. **Eleição** — `runCandidate` compara votos contra **maioria** (`majority()`).
2. **Commit** — entradas propostas com `MinAcks` omitido (ex.: cliente que não passe pelo HTTP normalizador) exigem ACKs em **réplicas = N**; entradas via HTTP típico levam já `MinAcks = N` quando o parâmetro está ausente/`0`; `raft_min_acks` permite baixar para [maioria, N] **por entrada**.

A configuração de runtime em `internal/cfg/cfg.go` cobre TTL de cache, checkpoint WAL, fsdb, etc.; **não** existe campo de quorum — o refinamento sugere algo no estilo das tags `env` já usadas lá.

Os comandos replicados são `commands.Data` (`Cmd`, `TableName`, `Item`), aplicados pela máquina de estados fora deste snippet; **`ProposeCommand` apenas anexa ao log**.

---

## 3. Semântica: o que pode ser “configurável” em segurança

### 3.1 Invariantes do Raft usual

Para o papel duplo deste projeto (eleição + replicação sobre o mesmo conjunto `{self} ∪ peers`):

- O **limite inferior seguro** para eleger líder **e** para marcar entrada como commitada continua sendo a **maioria estrita**: \(\lfloor N/2 \rfloor + 1\).
- Fixar um valor **inferior à maioria** para commit ou para votos atrai **vácuo de segurança** (dois líderes possíveis, commits incomparáveis) — **não** deve ser produto aceite sem novo protocolo.

### 3.2 O que faz sentido expor ao operador

1. **Eleição com maioria** — inalterável na implementação actual; não confundir com a barra de replicação até commit.
2. **Replicação até commit (`W = N` por defeito no HTTP)** — `raft_min_acks`/`min_acks` omitido ou zero → servidor preenche `MinAcks` com **todo o cluster** antes de propor; útil quando se quer máxima durabilidade antes de commitar (custo: um follower em baixo pode bloquear commits).
3. **`W` entre maioría e N só por entrada** — passando `raft_min_acks` (query) ou `min_acks`/`raft_min_acks` em JSON (**POST**) com valor válido **[maioria, N]**; relaxa disponibilidade **só dessa entrada** (não há baseline persistido no log neste desenho).
4. **`W` por operação ou por “classe”** — só faz sentido se **todas as réplicas** convergirem para o mesmo `W` efetivo no mesmo ponto linearizável do log (ver §6); caso contrário, o watermark único `commitIndex` do Raft não basta para regras conflituosas por entrada sem extensões.

**Suposições explícitas desta spec:** documentar **maioría** apenas para eleição; commit default “total” no caminho HTTP; possível variar relaxamento apenas **por entrada** com `MinAcks` explícito; evoluções futuras (env global A, comando dedicado só de config) permanecem em §4–5.

---

## 4. Abordagem A — variável de ambiente (global por processo)

### 4.1 Ideia

Cada processo `node` lê `QUORUM` (ou nome equivalente, ex.: `RAFT_COMMIT_QUORUM`) no arranque, valida contra \(N = \texttt{len(peers)} + 1\). Na implementação de referência (abordagem B no log), o commit usa **`effectiveRepMinAcks`** (omitir `MinAcks` ⇒ normalmente **N**); a eleição usa só **`majority()`**.

**Prós**

- Poucas mudanças: sem novos tipos de comando, sem entradas extras no WAL de aplicação.
- Comportamento homogéneo se **todas** as réplicas recebem o **mesmo** valor (contrato operacional).

**Contras**

- Alterar `W` exige **redeploy/restart coordenado** e acordo humano sobre o novo valor.
- Se um operador errar e subir nós com `W` diferentes, o cluster pode **não progredir** (ninguém atinge o commit mínimo que cada líder espera) ou, pior, se alguém violar o limite inferior de §3.1, **quebrar segurança** — a validação no arranque por nó é fundamental.

### 4.2 Integração sugerida no código

**1) `internal/cfg/cfg.go` — novo campo (exemplo)**

```go
// RaCommitQuorum é o número mínimo de nós (incluindo o líder) que devem
// ter replicado uma entrada antes de esta poder avançar commitIndex.
// 0 significa “replicação em todo o cluster (N)” alinhado ao default HTTP quando `RAFT_COMMIT_QUORUM` existir someday.
RaCommitQuorum int `env:"RAFT_COMMIT_QUORUM" env-default:"0" env-description:"min replicas for commit; 0 = full cluster N (HTTP default quando min_acks omitido)"`
```

Documentar que eleição continua sempre com `majority()` e só o commit pode usar um `W` mais estrito (ou default **N**) — comportamento já adoptado quando a abordagem B usa `effectiveRepMinAcks` apenas no caminho de commit. A decisão de produto numa eventual abordagem A pura por env seria:

- **Unificar** — um único `effectiveQuorum` para votos e commit (mais simples de raciocinar).
- **Só commit mais estrito** — funções `electionQuorum()` = maioria fixa; `commitQuorum()` = `max(maioria, W_cfg)`.

**2) `internal/raft/node.go` — estado e construtor**

```go
type Node struct {
	// ... campos existentes ...
	commitQuorum int // 0 => derivar de peers no primeiro uso
}

func NewNode(id string, peers []string, transport *Transport, logger *slog.Logger, commitQuorum int) *Node {
	// validar fora ou aqui: majority() <= commitQuorum <= len(peers)+1 quando commitQuorum > 0
	return &Node{
		// ...
		commitQuorum: commitQuorum,
	}
}
```

**3) Cálculo efetivo**

```go
func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) effectiveCommitQuorum() int {
	if n.commitQuorum <= 0 {
		return len(n.peers) + 1 // default: todo o cluster
	}
	if n.commitQuorum < n.majority() {
		// política: panic no New, ou degradar para majority com log ERROR — nunca silencioso
		return n.majority()
	}
	if n.commitQuorum > len(n.peers)+1 {
		return len(n.peers) + 1
	}
	return n.commitQuorum
}
```

Em `maybeAdvanceCommit`, usar `count >= minAcksRequiredToCommit(idx)` que combina `Data.MinAcks` explícito com `effectiveRepMinAcks` (ou pseudónimo `effectiveCommitQuorum` acima para abordagem A). Em `runCandidate`, continuar com `n.majority()` para eleição salvo decisão explícita de unificar votos com commit — **não** recomendado face ao Raft usual.

**4) `cmd/node/main.go`**

Após `cfg.Load()`, passar `cfg.Get().RaCommitQuorum` para `NewNode` / `NewNodeWithState`.

### 4.3 Testes mínimos sugeridos

- Com 3 nós (`N=3`), `W=3` — commit só após todas as réplicas; uma falha bloqueia commit (esperado).
- `W=0` — comportamento alinhado ao default actual: omitir `MinAcks` exige **N** ACKs até haver narrowing via log.
- Arranque com `W=1` num cluster de 3 — **deve falhar** na validação (nível pacote `cfg` ou `raft`).

---

## 5. Abordagem B — quorum via comando replicado (log)

### 5.1 Ideia

Tratar alteração de `W` como **estado da máquina de estados** replicada: uma entrada de log dedicada (novo `commands.Command`) aplica-se em **todas** as réplicas na mesma ordem, atualizando `currentW` utilizado em `maybeAdvanceCommit` (e opcionalmente em eleição).

**Prós**

- Mudanças **sem** reiniciar processos (após o comando commitado, todos aplicam o novo limite no mesmo índice lógico).
- Base para “vários níveis” no tempo (histórico explícito no WAL).

**Contras**

- **Persistência**: o valor ativo tem de sobreviver a restarts — ou está no log já carregado (`NewNodeWithState`), ou em metadados persistidos após apply.
- **Bootstrap**: primeiro valor de `W` ainda pode vir do env ou default até existir comando no log (definir regra clara — ver §5.3).
- **Complexidade**: validação no apply, compatibilidade com snapshots/checkpoints se no futuro truncarem log com estado derivado inconsistente.

### 5.2 Extensão do modelo de comandos

Modelo real em `internal/commands/cmd_data.go` (campos opcionais no mesmo envelope `Data`):

```go
type Data struct {
	Cmd               Commands `json:"cmd"`
	TableName         string   `json:"table_name"`
	Item              dto.Item `json:"item"`
	MinAcks           int      `json:"min_acks,omitempty"` // por entrada em [maj,N]; omitido/zero no HTTP ⇒ preenchido como N antes de propor
}
```

### 5.3 Fluxo

1. O operador envia comandos Raft via HTTP nas mesmas escritas (`POST /table`, `PUT /table/{name}`, `DELETE /table/{name}`), com opcional **`raft_min_acks`** ou **`raft_min_acks`/`min_acks`** no JSON (**POST**, conforme campo suportado).
2. Handler HTTP normaliza omitido/zero → `MinAcks = N`; líder propõe com `commands.Data.MinAcks` assim preenchido.
3. Após commit, só segue aplicar KV aos followers (**gRPC**/loop `Applied`).
4. **Bootstrap:** igual ao comportamento Raft genérico: quem propor com `MinAcks` omitido continua dependente apenas de código que preenche o valor (idealmente igual entre clientes ou camada HTTP única).

Nota histórica: versões mais antigas do repo suportaram `cluster_rep_min_acks` baseline no log — removido; campos extra no WAL antigos são ignorados na desserialização.

### 5.4 “Mais de um quorum” (interpretação do refinamento)

Duas leituras possíveis:

1. **Sequência temporal** — histórico de valores `W₁, W₂, …` ao longo do log (abordagem B natural).
2. **Simultâneo por tipo de operação** — ex.: `W_put` vs `W_delete` diferentes no mesmo índice de commit.

A (2) **não** se mapeia para o único `commitIndex` do Raft sem:

- ou **uma barreira global** `W_eff = max(W_put, W_delete, …)` para avançar commit (comportamento conservador),  
- ou **extensão do protocolo** (múltiplos streams de commit, outro consenso).

Recomendação desta spec: tratar (2) como **fora de escopo** até haver desenho formal; priorizar (1).

### 5.5 Apply no nó (implementação)

O consumidor de `Applied()` em `internal/api/cmds_grpc.go` aplica apenas mutações KV nos followers; líder volta cedo porque já aplicou no propose.

---

## 6. Comparação resumida

| Critério | A — Env global | B — Comando no log |
|----------|----------------|-------------------|
| Flexibilidade operacional | Baixa | Alta |
| Complexidade implementação | Baixa | Média–alta |
| Risco de config divergente entre nós | Alto se env mal gerido | Baixo **após** commit |
| Coerência com WAL/recovery | Só valor inicial; depois igual ao atual | Estado derivado deve acompanhar truncagem/snapshot |
| Adequação a “vários quorum” ao longo do tempo | Fraca | Boa |

---

## 7. Critérios de aceite sugeridos (para um PR futuro)

1. Pedidos HTTP sem `raft_min_acks` ou com valor `0`: `MinAcks` proposto == **N**; um follower em falta bloqueia o commit até voltar ou até uma escrita seguinte usar `raft_min_acks` estritamente menor que N (dentro de [maioria, N]).
2. Para `W > majority` explícito, commit **não** avança até `matchIndex` evidenciar `W` nós alinhados; teste com cliente simulador de falhas em `transport`.
3. Arranque com `W < majority` ⇒ **erro fatal** ou recusa explícita de subir listener de consenso quando validado em propose.

---

## 8. Ficheiros prováveis a tocar numa implementação

- `internal/cfg/cfg.go` — campo(s) env.
- `internal/raft/node.go` — `effectiveCommitQuorum`, `maybeAdvanceCommit`, opcionalmente `runCandidate`.
- `cmd/node/main.go` — injetar valor de cfg; loop de apply para comando de quorum se B.
- `internal/commands/*` — novo comando e payload se B.
- Testes em `internal/raft/...` e/ou `internal/wal/...` conforme recovery.

---

## 9. Limitações e próximos passos

- **Membership dinâmico:** alterar `peers` sem joint consensus altera `N` e portanto `majority()`; `W` persistido pode ficar inválido após remoção de nós — exigir recomputação ou comando de reconfiguração.
- **Observabilidade:** expor `effectiveCommitQuorum` e `clusterSize` no endpoint de estado do node para depuração.
- **Documentação operacional:** contrato de que **todos** os processos na abordagem A recebem o mesmo env antes de alterar `W`.

---

## 10. Rastreio à spec de refinamento

| Ideia em `refining/quorum_with_quorum.md` | Onde está tratado |
|------------------------------------------|-------------------|
| Config global por env (menos flexível, mais simples) | §4 |
| Config via comando de escrita (mais flexível) | §5 |
| Exemplo `Config` com tags `env` | §4.2 |
| Exemplo `ProposeCommand(commands.Data{...})` | §5.2–5.3 |
