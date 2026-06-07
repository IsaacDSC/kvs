---
description: Cria um novo ADR em docs/adr/ seguindo o esqueleto do projeto
argument-hint: [tema da decisão]
---

# Novo ADR (Architecture Decision Record)

Cria um **novo arquivo ADR** em `docs/adr/`, em **português**, seguindo o **mesmo esqueleto** do projeto (como em `docs/adr/0001-formato-serializacao-persistencia-kvs-generico.md`).

Tema pedido pelo usuário: $ARGUMENTS

## Antes de escrever

1. Liste `docs/adr/*.md` e determine o **próximo número** de 4 dígitos (`0002`, `0003`, …). Se a pasta estiver vazia ou não existir, use `0001`.
2. O nome do arquivo deve ser: `docs/adr/NNNN-titulo-curto-em-kebab-case.md` (sem acentos no slug; hífens entre palavras).

## Conteúdo obrigatório (template)

Use exatamente estas seções e marcadores; preencha com o tema que o usuário pediu (ou extraia do contexto da conversa):

```markdown
# ADR NNNN: <título descritivo em português>

- **Status:** Proposto
- **Data:** YYYY-MM-DD (use a data corrente do ambiente ou a data explícita do pedido)

## Contexto

<Problema, restrições, o que motivou a decisão. Tabelas ou listas quando ajudarem.>

## Decisão

<Lista numerada das decisões tomadas. Seja explícito: o que foi escolhido, o que foi rejeitado e por quê, quando aplicável.>

## Consequências

### Positivas

- ...

### Negativas / trade-offs

- ...

---

<Nota opcional de revisão: quando reabrir este ADR.>
```

**Regras:**

- **Status** inicial: `Proposto`, salvo se o usuário pedir outro (ex.: `Aceito`, `Substituído`).
- **Decisão** deve ser acionável e verificável; evite texto vago.
- Se faltarem dados, **pergunte ao usuário** o mínimo necessário antes de gravar o ficheiro; se o pedido já trouxer contexto suficiente, cria o ADR completo.
- Ao terminar, confirma o caminho do ficheiro criado e o número do ADR.
