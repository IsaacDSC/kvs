# Cache em memória (`memdb`): limite (LRU / bytes)

Este documento amplia as notas em `refining/00_mem_cache_LRU.md` com o estado atual do código (`internal/memdb`, `internal/db/facade.go`) e discute formas de implementação e tradeoffs. **Fora de âmbito aqui:** estruturas auxiliares ou bibliotecas específicas para política de eviction; garantias de eviction relativas ao pipeline durável (WAL/`fsdb`).

## Contexto no código atual

### `memdb.DB` e `memdb.Table`

- **`DB`** mantém um mapa `Tables map[string]*Table` protegido por `sync.RWMutex`; cada tabela tem o próprio `sync.RWMutex`.
- **`Table`** guarda valores serializados em `Data map[string][]byte` e um índice invertido **`SecondaryKey map[string]Set`** para consultas por SK.

Não há limite de entradas nem de bytes: o conjunto “quente” pode crescer até consumir toda a RAM disponível.

### `db.Facade` e papel do cache

O **`Facade`** serializa escrita com um único `sync.Mutex` e ordena operações assim:

- **`Set`**: WAL (`logdb`) → persistência (`fsdb`) → **`memdb`**.
- **`Get` / `GetBySecondaryKey`**: tentam **`memdb`** primeiro; em erro (por exemplo `ErrNotFound`), caem para **`fsdb`**.
- **`Delete`**: WAL → `fsdb` → remoção em **`memdb`**.

Ou seja, **`memdb` funciona como cache/write-through** em relação ao caminho durável (após `fsdb` completar com sucesso na escrita). Leituras podem repovoar o cache implicitamente apenas via `Set` no fluxo atual — não há “promoção” automática de uma linha lida do disco para `memdb` após um `Get` que fez fallback.

---

## Objetivos do refinamento (do documento de refining)

1. **Cap na memória** — por número de entradas (LRU), por **bytes** estimados, ou **combinação** (por exemplo máximo N chaves e máximo M MB).
2. **LRU** — boa localidade temporal; exige atualizar “último uso” em **`Get`** (e possivelmente em **`GetBySecondaryKey`** ao materializar entidades).
3. **Limite por bytes** — protege contra poucas chaves com valores enormes ou muitas chaves pequenas que somam demais.

---

## Dimensões de desenho e tradeoffs

### 1. Cache por tabela vs cache global

| Abordagem | Prós | Contras |
|-----------|------|---------|
| **Por tabela** | Isolamento: uma tabela com hotspot não esvazia outras; tuning por domínio (limites diferentes). | Mais estruturas e métricas; soma dos caps pode ultrapassar RAM se não houver orçamento global. |
| **Global (único LRU/heap)** | Controle firme do uso total de RAM; uma política única. | Competição entre tabelas; uma tabela pode dominar o cache inteiro; eviction precisa saber a qual `Table` pertence cada entrada para manter índices SK. |

**Nota de implementação:** como **`SecondaryKey`** é por `Table`, ao remover entradas por cap/eviction é preciso atualizar **`Data`** e, se aplicável, o índice SK da mesma forma que `Delete` — ou manter invariante “valor no mapa ⇒ SK indexado” (por exemplo decodificar para limpar SK como já faz `Delete`).

### 2. Limite por contagem vs por bytes vs ambos

| Política | Prós | Contras |
|----------|------|---------|
| **Só contagem** | Simples; custo O(1) ao atualizar uso. | Uma entrada gigante não é penalizada; RSS real pode variar muito. |
| **Só bytes** | Reflete uso de RAM dos blobs (`[]byte`) melhor. | Precisa manter `len(b)` por entrada ou estimativa; ao substituir valor em `Set`, ajustar contadores; entradas “caras” podem esvaziar o cache com poucas chaves (efeito esperado ou não). |
| **Contagem E bytes** | Teto duplo; típico em produção. | Dois gatilhos de eviction; ordem de decisão deve estar definida (por exemplo evict até satisfazer ambos). |

No **`Table.Set`**, o valor já é um `[]byte` — o custo por entrada está disponível para contabilização sem serializar de novo.

### 3. Onde registrar “uso recente” (touch)

Para LRU fazer sentido:

- **`Get(key)`** — deve marcar a entrada como recentemente usada (senão LRU degenera em “política por escrita apenas”).
- **`GetBySecondaryKey`** — hoje percorre chaves e chama `Get` por chave; um design ingênuo pode mover várias entradas para o topo em uma única consulta (amplifica churn). Alternativas: touch apenas dos valores retornados; ou política híbrida (touch limitado).

**Tradeoff:** mais touches aumentam contenda no lock da tabela e na estrutura de ordenação LRU; menos touches favorecem entradas “quentes” por SK sem refletir uso por PK.

### 4. Concorrência e locks

- **`Facade`** usa um **`Mutex` global**: serializa `Get`/`Set`/etc.; por ora não há competição entre goroutines na facade por operações concorrentes na mesma instância — código futuro pode relaxar isso.
- **`Table`** já tem `RWMutex`: operações que alterem ordem LRU ou removam entradas podem exigir lock exclusivo e bloquear leituras na mesma tabela.

**Tradeoff:** shards por tabela ou segmentos reduzem contenda, mas aumentam complexidade e podem prejudicar política global de RAM.

### 5. Repovoamento do cache no `Get` (opcional)

Se após eviction quiser manter bom hit rate:

- **`Get`** ao ler do **`fsdb`** poderia **`memdb.Set`** (ou “insert if under cap”) — introduz **writes** no caminho de leitura e interação com cap/eviction (possível “cache stampede” se muitas misses simultâneas).

**Tradeoff:** melhora latência de releituras; exige deduplicação ou semáforo por chave se necessário.

---

## Encaminhamentos de implementação (resumo)

1. **MVP:** cap por **contagem** + LRU por tabela; touch em **`Get`**; ao ultrapassar o cap, remover entradas mantendo **`Data`** e índice SK consistentes (equivalente lógico a **`Delete`** apenas em memória, sem WAL/`fsdb`).
2. **Refino:** acrescentar **limite em bytes** e política “evict até satisfazer ambos os tetos”.
3. **Integração com facade:** avaliar **promoção** opcional no fallback de **`Get`** (`fsdb` → `memdb`) para não degradar hit rate após liberação de memória; atenção a contenção e thundering herd.

---

## Referências no repositório

- Notas originais: `refining/00_mem_cache_LRU.md`
- Implementação atual: `internal/memdb/database.go`, `internal/memdb/table.go`
- Orquestração e ordem durável/cache: `internal/db/facade.go`
