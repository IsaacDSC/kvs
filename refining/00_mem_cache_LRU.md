# Não estourar memória: cap (LRU/bytes), fila limitada, eviction só após durable

**Cap na mem (LRU ou por bytes)**
    - Por que: o “hot set” cresce com o tráfego; sem limite, qualquer cache tende a consumir toda a RAM disponível.
    - LRU (Least Recently Used): quando precisa liberar espaço, remove entradas que não foram acessadas há mais tempo. Bom quando há localidade temporal (chaves repetidas).
    - Limite por bytes: além do número de entradas, conta o tamanho serializado dos valores (ou estimativa). Evita uma única entrada gigante ou muitas pequenas explodirem o uso real de RAM.
    - Combinação: por exemplo “no máximo N entradas e no máximo M MB”; eviction quando qualquer limite for atingido.


Na prática você mantém uma estrutura auxiliar (lista duplamente ligada + mapa, ou biblioteca de cache) por tabela ou global, dependendo do modelo.