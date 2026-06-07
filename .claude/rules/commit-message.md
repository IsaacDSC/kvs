# Mensagens de commit

Quando escreveres (ou sugerires) uma mensagem de **git commit** ou resumo/descrição curta de **PR**, segue o template do ficheiro `.gitmessage` na raiz do projeto (Conventional Commits, resumo em imperativo).

- **Primeira linha**: `tipo(escopo opcional): resumo` — até ~50 caracteres, imperativo, sem ponto final. **Tipo** só um destes: `feat`, `fix`, `docs`, `test`, `refactor`, `build`, `ci`.
- **Corpo** (se fizer sentido): uma linha em branco depois do resumo; explica *o quê* e *porquê*, não cada ficheiro linha a linha. Português ou inglês conforme o histórico da conversa; mantém o mesmo idioma até ao fim.
- **Átomos**: frases completas, gramática correta, sem ruído; tamanho proporcional à alteração.
- Não uses linhas de comentário `#` na mensagem final (isso é só no template do editor).
- Rodapés opcionais: `Fixes #…`, `BREAKING CHANGE: …`.
