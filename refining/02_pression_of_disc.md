# Menos pressão no disco: batch no fs + checkpoints

**Batch no fsdb**
    - Ideia: em vez de um write/fsync por chave no disco secundário, o worker acumula várias operações (por tempo, por contagem, ou por bytes) e persiste em uma rodada maior (um arquivo append, um rewrite de página, etc.).
    - Efeito: menos syscalls, menos seeks, melhor uso de throughput sequencial — especialmente em HDD ou SSD com write amplification alta.
    - Cuidados:
        - Ordem: se a ordem das operações importa (updates na mesma chave), o batch deve preservar ordem por chave ou aplicar a última versão de forma determinística.
        - WAL separado: o batch afeta só o checkpoint / estado em arquivo de dados; o WAL pode continuar uma entrada por operação para durabilidade fina. Recovery = replay WAL (e opcionalmente estado já batched no fs).
        - Latência: um batch “100 ms” adiciona até ~100 ms antes de aparecer no arquivo de dados — mas se leituras quentes vêm da mem, isso pode ser aceitável.

