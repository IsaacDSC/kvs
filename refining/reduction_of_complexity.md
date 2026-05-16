# Diminuição da complexidade na escrita e leitura

### Contexto 
O sistema tem como premissa priorizar a consistência e latencia do que a disponibilidade quando houver a partição

Esse sistema de banco de dados foi pensado para ser rápido e ser um kvs simples, e futuramente 
além de ter lock otimista ter também lock pessimista com ACID. 
Para busca pretente a ter somente busca por chave primária e chave secundária e não é feito,
para Petadados ou seja é um banco de configurações que deve se manter pequeno e médio quanto 
a busca de dados.
Esse banco de dados utiliza Raft com GRPC para comunicação entre os nodes, e futuramente terá 
Quorum configurável de escrita. Esse sistema de banco de dados expoem uma api rest para receber
as escritas e leituras.

## Objetivo
Consistência alta e baixa latência escrita e leitura no sistema. Atualmente estou tendo problemas
com a consistência de escrita/leitura entre fsdb e memdb.

## Duvidas pertinentes
#### Como manter a replicação com a consistência alta mantendo WAL + fsdb e memdb?

**Se manter sempre a escrita com fsdb e em segundo plano para memdb?**
    - A consistência seria alta e a performance compensaria?
    - Custo e complexidade para implementação?

#### Remover o memdb e ficar somente com fsdb e WAL
**Se manter agora somente o fsdb e deixar a simplicidade maior no sistema?** 
    - Teriamos muito impacto na performance em alta escala? se sim conseguiriamos metrificar? 
    - Custo de remoção de memdb do code e quanto de complexidade poderiamos diminuir?

