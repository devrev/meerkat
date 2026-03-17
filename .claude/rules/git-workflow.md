# Git Workflow

## Commit Message Format
```
<type>(<issue-id>): <description>

<optional body>

work-item: <issue-id>
```

Types: feat, fix, refactor, docs, test, chore, perf, ci

Example:
```
feat(ISS-271792): add notContains filter transformer

Implements the notContains operator for cube-to-DuckDB filter translation.
Handles single values, multiple values, null, and array type members.

work-item: ISS-271792
```

## DevRev Issue Linking

### Step 1: Extract Issue ID from Branch Name

Parse the current branch name for a DevRev issue ID. Valid patterns:
- `ISS-<digits>` — e.g., `ISS-271792`
- `<ALPHA>-<digits>` — e.g., `MULT-5`, `ENH-5039`

The ID can appear anywhere in the branch name:
- `ISS-271792-add-not-contains-filter` -> `ISS-271792`
- `feature/MULT-5-fix-null-handling` -> `MULT-5`
- `shriram/ISS-263625-regression-tests` -> `ISS-263625`

Extract using: `git branch --show-current | grep -oE '[A-Z]+-[0-9]+'`

### Step 2: Verify or Create Issue

**If issue ID found in branch name:**
1. Use `mcp__devrev__get_work` to verify the issue exists
2. Use the issue title/description for PR context
3. Add `work-item: <issue-id>` to commit message and PR description

**If NO issue ID found in branch name:**
1. Analyze the changes via `git diff main...HEAD`
2. Create a new DevRev issue using `mcp__devrev__create_work`:
   - `type`: `"issue"`
   - `title`: `"[Meerkat] <concise description of changes>"`
   - `body`: Detailed description of what changed and why
   - `applies_to_part`: `"don:core:dvrv-us-1:devo/0:capability/61"` (CAPL-61: Analytics Platform)
   - `owned_by`: `["don:identity:dvrv-us-1:devo/0:devu/2731"]`
3. Use the new issue's display_id (e.g., `ISS-XXXXXX`) in commit message and PR description

### Step 3: Include in Commit and PR

**Commit message** — append `work-item: <issue-id>` as a trailer:
```
feat(ISS-271792): add notContains filter transformer

work-item: ISS-271792
```

**PR description** — include in the body:
```markdown
## Summary
- Added notContains filter transformer
- Handles null values and array type members

## DevRev
work-item: ISS-271792

## Test plan
- [ ] Unit tests for single/multiple values
- [ ] Edge case: null values
- [ ] Edge case: `.` delimiter nested fields
```

## Pull Request Workflow

When creating PRs:
1. **Extract issue ID** from branch name (Step 1 above)
2. **Verify or create issue** in DevRev (Step 2 above)
3. Analyze full commit history: `git diff main...HEAD`
4. Draft comprehensive PR summary
5. Include `work-item: <issue-id>` in PR body
6. Include test plan with TODOs
7. Push with `-u` flag if new branch

> For the full development process (planning, TDD, code review) before git operations,
> see [development-workflow.md](./development-workflow.md).
