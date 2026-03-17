---
paths:
  - "**/*.ts"
  - "**/*.tsx"
  - "**/*.js"
  - "**/*.jsx"
---
# TypeScript/JavaScript Testing

> This file extends [common/testing.md](../common/testing.md) with TypeScript/JavaScript specific content.

## Test Framework

- **Jest** with `@swc/jest` transformer for fast TypeScript execution
- Run tests via Nx: `npx nx test <project-name>`
- Run all: `npx nx run-many --target=test --all`
- Watch mode: `npx nx test meerkat-core --watch`

## Test Patterns

- Prefer explicit SQL output assertions over snapshots
- Test generated SQL/AST structure directly
- Colocate tests in `__tests__/` directories or `*.spec.ts` files
- Cover edge cases: null values, empty arrays, special characters, nested `.` fields

## Agent Support

- **tdd-guide** - Use PROACTIVELY for new features, enforces write-tests-first
