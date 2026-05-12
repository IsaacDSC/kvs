# Log Sequence Number (LSN) Checkpoint WAL

**Checkpoints**
    - Problema: se só append no WAL, o arquivo de log cresce sem parar; no restart, replay fica linear no tamanho do WAL.
    - Checkpoint: ponto no qual você considera que o estado em fsdb (ou snapshot) já reflete tudo até um certo LSN (log sequence number) / offset no WAL.
    Depois do checkpoint: pode-se truncar ou arquivar o WAL até aquele ponto (ou rotacionar segmentos), porque o estado durável equivalente já está no fs em forma de dados + eventual meta do checkpoint.
    - Benefícios:
        - recovery mais rápido (menos replay);
        - menos espaço em disco usado pelo WAL;
        - menos “pressão” de I/O no boot.
    - Checkpoint vs batch: batch é como você escreve em massa; checkpoint é quando você declara “até aqui está refletido no fs, posso enxugar o log”. Na prática costumam aparecer juntos: batches preenchem o estado no fs; checkpoints marcam até onde o WAL pode ser podado.

<!-- VALIDAR SE não seria já algo como essa implementação: internal/durable/checkpoint.go -->