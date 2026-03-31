# ADR 0002: Estratégia de persistência (memória, disco e distribuição com baixa latência e consistência alta)

- **Status:** Proposto
- **Data:** 2026-03-30

## Contexto

O KVS opera **hoje em memória**; os requisitos evoluem para:

- **dados em memória** no caminho quente (leituras e escritas com latência mínima),
- **durabilidade em disco** (sobrevivência a falhas de processo e, em parte, à máquina),
- **distribuição** com **latência muito baixa** e **consistência alta** entre nós.

Há tensão natural entre velocidade (menos I/O síncrono, batching, flush assíncrono) e garantias fortes (cada escrita visível e durável antes de “ok” ao cliente, ou antes de propagar réplicas). Opções discutidas incluem:

- **Estrutura em log** (append-only / WAL): ordem total de operações, recuperação por replay, base comum em sistemas duráveis.
- **Persistir “como em memória”**: snapshot do estado atual (ex.: tabelas/estruturas serializadas) versus **registro de mutações** — snapshots simplificam leitura fria; logs simplificam durabilidade incremental e compactação posterior.
- **Memória primeiro + replicação/flush assíncrono para disco**: reduz latência percetiva do cliente, mas **até o `fsync`/equivalente completar**, há janela de perda em crash; “alta consistência” exige definir **consistência entre processos** (linha de recuperação) e **entre nós** (modelo de réplicação).

O [ADR 0001](./0001-formato-serializacao-persistencia-kvs-generico.md) já fixa o **formato de serialização** em disco (MessagePack/CBOR como primário, JSON auxiliar, versionamento de registro). Esta ADR trata do **onde e quando** persistir e como evoluir para cluster, **sem** duplicar o codec.

## Decisão

1. **Modelo em duas camadas (conceitual):** separar **estado servido** (estruturas em RAM, alinhadas à API atual) de **histórico durável** no disco. O caminho quente atualiza RAM; o disco recebe **append de operações ou batches** derivados dessas mutações (não obrigar que o layout binário em disco seja idêntico ao das estruturas em memória — apenas que a **semântica** de recuperação seja reproduzível).

2. **Log como eixo de durabilidade:** adotar **append-only log** (WAL) como **fonte de verdade** para recuperação após falha: ordem estável de entradas, possivelmente com **checkpoint/snapshot periódico** para limitar tempo de replay (combina com a ideia de “estrutura de logs” e evita regravar o estado inteiro a cada escrita).

3. **Política de flush (explícita por fase):**
   - **Fase inicial (disco local, forte durabilidade por escrita):** cada `Put`/`Delete` relevante deve **append** ao WAL e **sincronizar** com a política configurável (ex.: `fsync` por escrita vs. batch com janela de perda documentada). Latência mais alta que RAM pura, mas **perda em crash** previsível.
   - **Fase otimizada (latência muito baixa):** permitir **buffer em memória** + flush assíncrono **desde que** o contrato com o cliente declare claramente o **nível de durabilidade** (ex.: “ack após grupo/commit” vs. “ack após RAM”). Não misturar semânticas no mesmo modo sem documentação.

4. **Replicação e consistência no cluster:** para **consistência alta** entre nós, rejeitar apenas “best-effort assíncrono” como modo **padrão**; o caminho padrão alvo é **consenso ou replicação síncrona de quorum** (ex.: líder + followers com confirmação majoritária antes do ack, estilo Raft) **ou** um modelo formalmente equivalente documentado. Escrita assíncrona para **followers** só como modo **explícito** com garantias relaxadas.

5. **Ordem de implementação sugerida (verificável):** (a) WAL local + recuperação na subida; (b) checkpoints; (c) métricas de latência e política de sync configurável; (d) camada de cluster com contrato de consistência alinhado ao item 4.

6. **Snapshot “igual à memória”:** usar como **suplemento** (checkpoint) ao log, não como substituto único do WAL, salvo análise que prove recuperação aceitável e custo de I/O compatível com os requisitos.

## Consequências

### Positivas

- Log append oferece serialização natural de operações, boa compressão temporal de escritas e base clara para replicação.
- Separamos codec (0001) de política de I/O e de cluster; decisões futuras de throughput podem tunar batch/`fsync` sem redesenhar o modelo mental.
- O roadmap memória → disco → distribuído fica traçado com marcos testáveis (recovery, checkpoint, quorum).

### Negativas / trade-offs

- **Baixa latência + alta consistência + durabilidade forte** no mesmo ack é fisicamente limitada pela rede e pelo disco; será necessário **escolher** (ou expor) modos: ex. ack rápido com risco de perda vs. ack após quorum + sync.
- WAL + checkpoints aumentam complexidade operacional (compactação do log, espaço em disco, testes de crash).
- Cluster com quorum adiciona latência mínima de ida e volta comparado a nó único em RAM.

---

*Revisar esta ADR ao implementar WAL (formato de entrada, rotação), ao definir configuração padrão de `fsync`, e ao escolher biblioteca/protocolo de consenso ou desenho de replicação.*
