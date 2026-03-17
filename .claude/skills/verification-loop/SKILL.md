---
name: verification-loop
description: "A comprehensive verification system for Claude Code sessions."
origin: ECC
---

# Verification Loop Skill

A comprehensive verification system for Claude Code sessions.

## When to Use

Invoke this skill:
- After completing a feature or significant code change
- Before creating a PR
- When you want to ensure quality gates pass
- After refactoring

## Verification Phases

### Phase 1: Build Verification
```bash
npx nx run-many --target=build --all --parallel 2>&1 | tail -20
```

If build fails, STOP and fix before continuing.

### Phase 2: Type Check
```bash
npx tsc --noEmit 2>&1 | head -30
```

Report all type errors. Fix critical ones before continuing.

### Phase 3: Lint Check
```bash
npx nx lint meerkat-core 2>&1 | head -30
```

### Phase 4: Test Suite
```bash
# Run tests for affected project(s)
npx nx test meerkat-core 2>&1 | tail -50

# Run all tests
npx nx run-many --target=test --all 2>&1 | tail -50

# Target: 80% minimum coverage
```

Report:
- Total tests: X
- Passed: X
- Failed: X
- Coverage: X%

### Phase 5: Security Scan
```bash
# Check for secrets
grep -rn "sk-" --include="*.ts" --include="*.js" . 2>/dev/null | head -10
grep -rn "api_key" --include="*.ts" --include="*.js" . 2>/dev/null | head -10

# Check for console.log
grep -rn "console.log" --include="*.ts" --include="*.tsx" packages/ 2>/dev/null | head -10
```

### Phase 6: Diff Review
```bash
git diff --stat
git diff HEAD~1 --name-only
```

Review each changed file for:
- Unintended changes
- Missing error handling
- SQL injection risks (string concatenation instead of AST)
- Potential edge cases (null values, `.` delimiters, empty arrays)

## Output Format

After running all phases, produce a verification report:

```
VERIFICATION REPORT
==================

Build:     [PASS/FAIL]
Types:     [PASS/FAIL] (X errors)
Lint:      [PASS/FAIL] (X warnings)
Tests:     [PASS/FAIL] (X/Y passed, Z% coverage)
Security:  [PASS/FAIL] (X issues)
Diff:      [X files changed]

Overall:   [READY/NOT READY] for PR

Issues to Fix:
1. ...
2. ...
```

## Continuous Mode

For long sessions, run verification every 15 minutes or after major changes:

```markdown
Set a mental checkpoint:
- After completing each function
- After finishing a component
- Before moving to next task

Run: /verify
```

## Pre-Push Gate (MANDATORY)

Before ANY `git push`, run the full monorepo test suite:

```bash
npx nx run-many --target=test --all 2>&1 | tail -50
```

**Rules:**
- If ANY test fails, DO NOT push. Fix the failing tests first.
- This applies to all branches, not just main.
- Run this even if only one package was changed — cross-package regressions are common.
- After fixing failures, re-run the full suite to confirm before pushing.

## Integration with Hooks

This skill complements PostToolUse hooks but provides deeper verification.
Hooks catch issues immediately; this skill provides comprehensive review.
