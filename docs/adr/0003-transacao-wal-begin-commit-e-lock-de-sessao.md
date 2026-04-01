# ADR 0003: Transação no WAL (BEGIN/COMMIT) e lock de sessão até o fim da `NewSession`

- **Status:** Aceito
- **Data:** 2026-04-01

## Contexto

Historicamente a sessão podia ser apenas **lock por tabela** com `Set`/`Delete` imediatos; isso **não** dava atomicidade entre várias escritas. A decisão abaixo fixa sessão **transacional** (`NewSession` com staging + WAL BEGIN…COMMIT) mantendo o lock até ao fim do commit.

Foram consideradas alternativas descritas em `docs/specs/transaction_session.md`, entre elas:

- staging + aplicar em memória antes de persistir (arriscado para consistência com disco);
- vários `Append` sem marca de transação (permite WAL parcial após restart);
- registro em batch único no WAL;
- cópia da tabela (COW) para leituras isoladas.

Precisamos de uma decisão **acionável** para o próximo passo de implementação: a opção **mais simples que ainda garanta semântica durável tudo-ou-nada** (ou seja, após restart, ou a transação inteira reaparece no replay, ou nenhuma parte dela), sem abandonar o **lock de sessão** que já serializa trabalho por tabela.

## Decisão

1. **Adotar a “Abordagem 3”** do spec: **WAL transacional com `BEGIN` e `COMMIT`** (identificador de transação, por exemplo `txid`), de modo que o replay **só aplique mutações de transações com `COMMIT` gravado**; transações incompletas (erro no meio, crash antes do commit) **não** produzem efeito durável.

2. **Rejeitar, para este objetivo**, soluções que apenas ordenem “persistir depois aplicar” **sem** `BEGIN/COMMIT` (ou batch atômico equivalente), porque **não** garantem tudo-ou-nada no disco quando um append falha a meio — o WAL pode ficar parcialmente escrito e o replay aplicaria só o prefixo.

3. **Semântica de commit no processo:** ao final de `fn(tx)`, se não houver erro (e o contexto permitir), gravar no WAL a sequência **`BEGIN` → mutações → `COMMIT`** (e política de `Flush`/`Sync` alinhada ao que definirmos para “commit durável”). Só então aplicar em memória de forma consistente com essa ordem (ou conforme desenho fino acordado no PR de implementação), de modo que **rollback** = não emitir `COMMIT` e descartar staging — **nada** fica comprometido no WAL para essa transação.

4. **Lock de sessão:** manter o **mesmo lock de sessão por tabela** que `NewSession` já usa: o **lock permanece até o fim da execução de `fn`** (e do tratamento de erro/timeout), **incluindo** a fase em que o commit WAL (e eventual aplicação em memória) ocorre. Ou seja: **não** liberar o semáforo de sessão antes de concluir commit ou rollback lógico da transação; outra sessão na mesma tabela **não** intercala no meio.

5. **Escopo inicial:** transação **por tabela** (alinhado ao lock atual); **não** exigir neste ADR atomicidade multi-tabela num único commit (pode ser ADR futura).

6. **Compatibilidade:** evolução do formato do WAL (versionamento / replay dual) será tratada na implementação; este ADR fixa apenas a **escolha arquitetural** (BEGIN/COMMIT + lock até o fim da sessão).

## Consequências

### Positivas

- **Tudo-ou-nada durável** no modelo WAL: sem `COMMIT`, o replay não aplica a transação — adequado a crash entre mutações.
- **Rollback simples** em runtime: basta não escrever `COMMIT`.
- **Lock de sessão** continua a serializar o fluxo inteiro (lógica do usuário + persistência do commit), evitando corridas com outra sessão na mesma tabela.
- A decisão é **verificável**: testes de falha simulada (erro antes do commit, truncagem) e replay devem mostrar ausência de efeito parcial para transações não commitadas.

### Negativas / trade-offs

- **Mudança de formato do WAL** e lógica de replay mais elaborada (buffers por `txid` até ver `COMMIT`).
- **Memória** no replay proporcional ao tamanho da maior transação em voo (até o commit).
- Transações **grandes** aumentam latência de commit e tamanho de escrita; pode ser necessário limitar tamanho ou evoluir para batch (ADR futura) se virar problema.
- **Multi-tabela** num único commit continua fora do escopo desta decisão; exigiria coordenação extra.

---

*Revisar esta ADR quando a implementação estiver mergeada, quando houver necessidade de transações multi-tabela, ou quando benchmarks indicarem necessidade de registro em batch em vez de vários appends dentro do mesmo commit lógico.*
