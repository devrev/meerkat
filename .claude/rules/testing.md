# Testing Requirements

## Minimum Test Coverage: 80%

Test Types:
1. **Unit Tests** - SQL AST generation, filter transformers, utility functions
2. **Integration Tests** - DuckDB query execution (meerkat-node, meerkat-browser)
3. **Regression Tests** - Specific bug fixes with issue references

## Test Commands (Nx Monorepo)

```bash
npx nx test meerkat-core                        # Test one project
npx nx run-many --target=test --all             # Test all
npx nx test meerkat-core --watch                # Watch mode
npx nx test meerkat-core --testPathPattern="filter"  # Specific tests
```

## Test-Driven Development

MANDATORY workflow:
1. Write test first (RED)
2. Run test - it should FAIL
3. Write minimal implementation (GREEN)
4. Run test - it should PASS
5. Refactor (IMPROVE)
6. Verify coverage (80%+)

## Troubleshooting Test Failures

1. Use **tdd-guide** agent
2. Check test isolation
3. Verify mocks are correct
4. Fix implementation, not tests (unless tests are wrong)
