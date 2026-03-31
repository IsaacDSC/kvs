# ADR 0001: Formato de serialização para persistência (KVS com valores genéricos)

- **Status:** Aceito
- **Data:** 2026-03-30

## Contexto

Precisamos persistir valores **genéricos** (`any` / estruturas arbitrárias) num KVS em que o **cliente da biblioteca não deve depender de tipagem forte** nem de schemas distribuídos. Os objetivos são **menor tamanho em disco**, **encode/decode rápidos** e **simplicidade** sem gerar código a partir de `.proto` / `.avsc`.

Formatos centrados em schema (Protobuf, Avro na forma usual) funcionam bem com contrato versionado compartilhado. Para “qualquer coisa JSON-compatível em memória”, o codec deve mapear naturalmente para:

- mapas, listas, string, números, bool, nil
- opcionalmente `[]byte` e tipos de tempo, conforme a API expuser

Isso favorece formatos **auto-descritivos** ou **sem schema obrigatório**.

### Comparativo (tendências)

| Formato     | Tamanho   | Velocidade | Schema obrigatório | Adequação a `any` genérico |
| ----------- | --------- | ---------- | ------------------ | -------------------------- |
| JSON        | Maior     | Mais lenta | Não                | Excelente                  |
| MessagePack | Menor     | Rápida     | Não                | Excelente                  |
| CBOR        | Menor     | Rápida     | Não*               | Excelente                  |
| BSON        | Intermed. | Rápida     | Não                | Boa (tipos extras Mongo)   |
| Protobuf    | Muito compacto** | Muito rápido** | Sim (na prática) | Fraca sem wrapper/schema |
| Avro        | Compacto** | Rápido**  | Sim                | Fraca para cliente “livre” |

\* CBOR pode usar schemas (CDDL), mas não é obrigatório para uso documento-like.  
\** Com mensagens tipadas e schema fixo; genérico vira `Struct`/`bytes` e perde parte da vantagem.

**JSON:** interoperável e debugável; porém maior payload e parsing mais pesado; em Go, `json.Unmarshal` para `any` usa `float64` para números se não houver cuidado com decimais grandes.

**MessagePack:** binário “JSON-like”, sem schema, bom para `map[string]any`; menos legível que JSON; nuances entre implementações (ext types) — escolher uma lib e documentar.

**CBOR ([RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)):** padrão IETF, compacto, sem schema documento-like; ecossistema Go um pouco menos uniforme que MsgPack para KV simples.

**BSON:** útil se tipos estendidos ou interoperabilidade Mongo forem requisitos; senão MsgPack/CBOR tendem a ser mais neutros.

**Protobuf / Avro:** excelentes com mensagens tipadas; para valores genéricos costumam cair em `Struct`/`Value`/`Any`/`bytes`+JSON, anulando ganhos e complicando a API — peso operacional alto (registry, governança) para lib KV genérica.

## Decisão

1. **Codec primário de persistência:** **MessagePack** ou **CBOR** — menor tamanho e maior throughput que JSON para estruturas aninhadas típicas, sem `.proto` ou registry de schema, alinhado a `map[string]any` / `any`. MessagePack é o candidato principal por simplicidade no ecossistema Go; CBOR se preferir **padronização IETF** explícita.

2. **JSON** permanece como **opção** (export/import, depuração, compatibilidade com sistemas somente texto): segundo modo ou formato legado.

3. **Não** adotar Protobuf nem Avro como **único** formato se a API prometer valores arbitrários sem contrato — salvo envelope explícito (ex.: `bytes` + tipo interno, ou registro com versão de codec no cabeçalho).

4. **Versionar registros no disco:** prefixo ou header pequeno com `version` + `codec` para evoluir formato sem quebrar leituras antigas.

5. **Números em Go:** documentar comportamento (especialmente inteiros grandes e `float64` via JSON); binários podem preservar inteiros com larguras distintas — definir política.

6. **BSON** só se tipos estendidos ou integração Mongo forem requisitos explícitos.

**Resumo:** persistência binária padrão MessagePack (ou CBOR); JSON como alternativa legível; Protobuf/Avro reservados a integrações com schema first-class.

## Consequências

### Positivas

- Tamanho e velocidade melhores que JSON para a maioria dos payloads sem impor contrato ao cliente.
- API continua aceitando estruturas genéricas sem codegen ou schema distribuído.
- JSON opcional preserva interoperabilidade humana e ferramentas.

### Negativas / trade-offs

- Payloads binários exigem ferramentas para inspeção (não são “cat-friendly” como JSON).
- MessagePack/CBOR: escolher implementação e documentar ext types / tags se usados.
- Benchmarks e libs reais devem validar a escolha com payloads representativos e versão de Go usada no projeto.

---

*Revisar esta ADR quando houver dados de benchmark ou mudança nos requisitos (ex.: obrigatoriedade de schema, integração Mongo/Kafka).*
