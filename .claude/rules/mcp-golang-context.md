# MCP Golang Context First

Para qualquer tarefa de código neste projeto, **se a ferramenta MCP `golang_project_context` estiver disponível na sessão**, chama-a antes de propor ou implementar mudanças de código. Se a ferramenta não estiver disponível, segue normalmente usando `go.mod`, layout do repositório e `CLAUDE.md` como contexto.

## Fluxo exigido (quando a ferramenta existe)

1. Chamar `golang_project_context` primeiro **com argumentos padrão**: passar `{}` (objeto vazio) ou omitir todos os campos opcionais. **Não** definir `workspace_path` com um caminho do host (ex.: `/Users/...` no macOS) quando o servidor MCP roda em Docker ou isolado desse filesystem — isso quebra a execução (`-32603`).
2. Só passar `workspace_path` quando for um caminho **dentro** do runtime do MCP (mesmos mounts/container/workdir do servidor). Na dúvida, usar só a invocação padrão.
3. Resumir o contexto relevante retornado (módulo, versão de Go, layout, sinais de qualidade, orientações).
4. Usar esse contexto como restrições para decisões de implementação e revisão.

## Enforcement

- Não iniciar a implementação antes de usar a ferramenta (quando disponível).
- Se a ferramenta falhar, reportar a falha e perguntar se deve tentar de novo ou prosseguir sem ela.
- Manter recomendações alinhadas com Effective Go e as convenções do projeto em `specs/plano-implementacao-mcp-contexto-golang.md`.
