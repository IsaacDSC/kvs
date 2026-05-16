# C. Checkpoint do WAL vs diretórios ainda não criados no fsdb

O erro `open tmp/node1/test_tb/key/fordel: no such file or directory` indica que, no momento do flush, a **árvore de diretórios** esperada pelo fsdb para essa chave **ainda não existia**.

Possíveis contribuintes:

- **Ordem entre criação de tabela / primeira escrita / flush periódico:** se o flush do checkpoint ocorrer numa janela em que a operação de `Set` ainda não criou todos os parents no disco (ou batcher diferido atrasa a materialização).
- **Impacto de FS_DEFER_WRITES / batching:** escrita ao disco pode ficar pendente; o checkpoint tenta consolidar estado que assume caminhos já existentes.

Este erro **avisou** antes do `optimistic_set_cmd` falhar; pode ser sintoma de **pressão de I/O** ou de **ordem** entre componentes, e em conjunto com **(A)** pode deixar réplicas **desalinhadas** nas versões.


