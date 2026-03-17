---
paths:
  - "**/*.ts"
  - "**/*.tsx"
  - "**/*.js"
  - "**/*.jsx"
---
# TypeScript/JavaScript Hooks

> This file extends [common/hooks.md](../common/hooks.md) with TypeScript/JavaScript specific content.

## PostToolUse Hooks

Configure in `.claude/settings.json` or `~/.claude/settings.json`:

- **Prettier**: Auto-format JS/TS files after edit
- **TypeScript check**: Run `tsc` after editing `.ts`/`.tsx` files
- **console.log warning**: Warn about `console.log` in edited files
- **Nx lint**: Run `npx nx lint <affected-project>` after edits

## Stop Hooks

- **console.log audit**: Check all modified files for `console.log` before session ends
- **Test affected**: Run `npx nx test` for affected projects before session ends
