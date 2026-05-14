# Especificação: pacote `db` (fachada) entre `store` e `memdb`

## Objetivo

Introduzir um pacote `db` como **fachada** entre a aplicação e o motor em memória (`memdb`) e a persistência (`store`).

Neste primeiro passo, o foco é **documentar o uso de interfaces para separação de camadas** e garantir que o pacote `db` **só repassa (forward) chamadas para `memdb`**. A persistência continua sendo responsabilidade do `store`, que injeta durabilidade no `memdb` via interface.

**Princípios de escopo:**

- O `db` não deve implementar WAL/recovery/checkpoint nem conhecer detalhes do disco neste passo.
- O acoplamento entre persistência e motor deve ocorrer via interfaces já existentes no `memdb` (`memdb.DurableWriter` e `memdb.TransactionCommitter`).
- Manter simplicidade: evitar refactors grandes enquanto a fachada `db` ainda delega integralmente ao `memdb`.

---

## Estado atual (resumo)

- **`internal/memdb`:** estado em RAM (`DB`, `Table`, sessões, locking). Expõe `DurableWriter` e `TransactionCommitter` para encaminhar mutações a quem persiste.
- **`internal/store`:** WAL + checkpoint + replay; possui `*memdb.DB` interno e implementa `memdb.DurableWriter` (e commit transacional). `main` e testes acoplam-se a `store.Open` + `s.DB()`.

Ou seja: a **direcção de dependência** é hoje `store` → `memdb` (concreto). O diagrama alvo sugere **`db` encapsulando `memdb`**, enquanto o `store` continua a ser o backend de persistência por enquanto.

---

## Visão alvo (neste passo)

- A aplicação passa a importar o pacote `db` e não importa `memdb` diretamente.
- O pacote `db` atua como **fachada**: `db.DB` encapsula um `*memdb.DB` e delega operações de leitura/escrita para os métodos existentes de `memdb`.
- O `store` continua responsável por persistência e recovery, e deve fazer a injeção de durabilidade no `memdb` (por exemplo via `memdb.DB.SetDurable(...)`) antes da fachada `db` ser usada.

Nesta primeira fase, não existe implementação nova de disco dentro do pacote `db` — a persistência continua sendo a do `store` atual.

---

## Uso de interfaces e fachada `db` (forward-only)

### A) Interface de durabilidade no `memdb` + façade `db` (forward-only)

**Ideia:** o `memdb` define interfaces para persistência (`memdb.DurableWriter` e `memdb.TransactionCommitter`). O `store` implementa esses contratos e injeta o writer numa instância de `memdb.DB` após o recovery do WAL.

Nesta fase, o pacote `db` é apenas uma camada de apresentação: ele encapsula um `*memdb.DB` e expõe `db.DB`/`db.Table` delegando diretamente para os métodos existentes do `memdb`. Não há nova lógica de disco dentro do `db` ainda.

---

### B) Forward para `memdb` agora (o `db` não cria novos backends)

**Ideia:** o `store` continua a ser o backend de persistência; o `memdb` continua sendo o motor em RAM. O papel do `db` nesta fase é **encapsular** e **repassar**: a API usada pela aplicação passa a ser `db` (fachada) mas a execução real continua a ser nos métodos do `memdb`.

Nesta fase, evitamos desenhar APIs de abertura (`OpenMemory`, `OpenFile`) ou introduzir um segundo engine: o objetivo é apenas separar os imports e explicitar a fronteira via interfaces de durabilidade do `memdb`.

---

### C) Separação engine/pager (adiada)

Fora do escopo desta fase. O objetivo agora é apenas a fachada `db` e a clareza de responsabilidades:

- `memdb` executa a lógica em RAM;
- `store` persiste e injeta durabilidade via interfaces;
- `db` apenas encapsula/repassa para `memdb`.

---

### D) Encapsulamento: a app depende de `db`, não de `memdb`

**Ideia:** a fachada `db` deve esconder os detalhes do motor `memdb`. O `store` continua existindo como backend de persistência, mas o código da aplicação deve passar a depender de `db` e não de `internal/memdb`.

Nesta fase, “esconder” significa principalmente encapsular o `*memdb.DB` e repassar para os métodos do `memdb`.

---

## Riscos comuns

- **Interface grande demais:** tudo vira um `God interface`; cada backend arrasta métodos que não usa.
- **Interface pequena demais:** a fachada `db` pode não suportar toda a API que a aplicação precisa, forçando refactor futuro.
- **Dependência circular:** `db` não deve importar `store` **concreto**; deve depender apenas de contratos de interface (injeção) para evitar ciclos.
- **Duplicação de tipos:** `Item`, erros e codecs devem viver num sítio único (hoje `memdb` + `code`).

---

## Plano de ação (somente fachada `db`)

Fases pequenas e verificáveis:

1. Criar `internal/db` com a API/fachada que a aplicação vai usar (`db.DB`, `db.Table` e tipos/erros que vocês decidirem expor).
2. Definir se `db` usa wrappers finos ou re-exports/aliases (tradeoff entre encapsulamento e simplicidade).
3. Ajustar o “ponto de entrada” da aplicação para obter uma instância de `db.DB` que encapsula um `*memdb.DB` já com durabilidade injetada (evitar depender de `s.DB()` no `main`).
4. Atualizar `main.go` e testes para dependerem de `db` (e executar `go test ./...` para garantir que WAL + replay permanecem invariantes).

---

## Relação com documentos existentes

- [estrategia_persistencia_disco.md](./estrategia_persistencia_disco.md) cobre **fases de WAL/recovery**; este documento cobre **fronteiras entre pacotes** e como a fachada `db` evita dependência direta de `memdb`.
- ADRs de serialização e persistência continuam a reger **formato em bytes**; o pacote `db` regeria **API e composição**, não o codec.

---

## Decisões em aberto (para fechar antes de codificar `db`)

1. O **`Table`** permanece tipo exportado em `memdb` ou passa a `db.Table` (alias)?
2. Erros de domínio ficam em `db`, `memdb`, ou pacote `kvserr` partilhado?
3. Como o `store` vai expor (ou ajudar a obter) uma instância de `db.DB` já com durabilidade injetada (e com ciclo de vida/`Close()` coerente)?

Responder a estas três questões evita refactors grandes durante a integração da fachada `db`.
