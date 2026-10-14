---
name: coding-standards
description: "Enforces TypeScript/React/Node.js coding conventions for the Meerkat SDK: naming patterns, immutability rules, error handling, and project structure. Use when reviewing code quality, starting new modules, refactoring for consistency, or onboarding contributors to repo conventions."
---

# Coding Standards

Project-specific coding conventions for the Meerkat SDK (TypeScript, React, Node.js, Nx monorepo).

## When to Activate

- Reviewing code for naming, formatting, or structural consistency
- Starting a new module or package in the monorepo
- Refactoring existing code to follow project conventions
- Onboarding new contributors to repo-specific patterns
- Setting up or updating linting and type-checking rules

## TypeScript/JavaScript Standards

### Variable Naming

```typescript
// ✅ GOOD: Descriptive names
const marketSearchQuery = 'election'
const isUserAuthenticated = true
const totalRevenue = 1000

// ❌ BAD: Unclear names
const q = 'election'
const flag = true
const x = 1000
```

### Function Naming

```typescript
// ✅ GOOD: Verb-noun pattern
async function fetchMarketData(marketId: string) { }
function calculateSimilarity(a: number[], b: number[]) { }
function isValidEmail(email: string): boolean { }

// ❌ BAD: Unclear or noun-only
async function market(id: string) { }
function similarity(a, b) { }
function email(e) { }
```

### Immutability Pattern (CRITICAL)

```typescript
// ✅ ALWAYS use spread operator
const updatedUser = {
  ...user,
  name: 'New Name'
}

const updatedArray = [...items, newItem]

// ❌ NEVER mutate directly
user.name = 'New Name'  // BAD
items.push(newItem)     // BAD
```

### Error Handling

```typescript
// ✅ GOOD: Comprehensive error handling
async function fetchData(url: string) {
  try {
    const response = await fetch(url)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Fetch failed:', error)
    throw new Error('Failed to fetch data')
  }
}

// ❌ BAD: No error handling
async function fetchData(url) {
  const response = await fetch(url)
  return response.json()
}
```

### Async/Await Best Practices

```typescript
// ✅ GOOD: Parallel execution when possible
const [users, markets, stats] = await Promise.all([
  fetchUsers(),
  fetchMarkets(),
  fetchStats()
])

// ❌ BAD: Sequential when unnecessary
const users = await fetchUsers()
const markets = await fetchMarkets()
const stats = await fetchStats()
```

### Type Safety

```typescript
// ✅ GOOD: Proper types
interface Market {
  id: string
  name: string
  status: 'active' | 'resolved' | 'closed'
  created_at: Date
}

function getMarket(id: string): Promise<Market> {
  // Implementation
}

// ❌ BAD: Using 'any'
function getMarket(id: any): Promise<any> {
  // Implementation
}
```

## React Conventions

### Component Structure

```typescript
// ✅ GOOD: Functional component with types
interface ButtonProps {
  children: React.ReactNode
  onClick: () => void
  disabled?: boolean
  variant?: 'primary' | 'secondary'
}

export function Button({
  children,
  onClick,
  disabled = false,
  variant = 'primary'
}: ButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`btn btn-${variant}`}
    >
      {children}
    </button>
  )
}
```

### Custom Hooks

```typescript
// ✅ GOOD: Reusable custom hook
export function useDebounce<T>(value: T, delay: number): T {
  const [debouncedValue, setDebouncedValue] = useState<T>(value)

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedValue(value)
    }, delay)

    return () => clearTimeout(handler)
  }, [value, delay])

  return debouncedValue
}

// Usage
const debouncedQuery = useDebounce(searchQuery, 500)
```

### State Updates

```typescript
// ✅ Functional update for state based on previous state
setCount(prev => prev + 1)

// ❌ Direct state reference — can be stale in async scenarios
setCount(count + 1)
```

### Conditional Rendering

```typescript
// ✅ GOOD: Clear conditional rendering
{isLoading && <Spinner />}
{error && <ErrorMessage error={error} />}
{data && <DataDisplay data={data} />}

// ❌ BAD: Ternary hell
{isLoading ? <Spinner /> : error ? <ErrorMessage error={error} /> : data ? <DataDisplay data={data} /> : null}
```

## API Response Format

```typescript
interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: string
  meta?: {
    total: number
    page: number
    limit: number
  }
}
```

### Input Validation

```typescript
import { z } from 'zod'

const CreateMarketSchema = z.object({
  name: z.string().min(1).max(200),
  description: z.string().min(1).max(2000),
  endDate: z.string().datetime(),
  categories: z.array(z.string()).min(1)
})
```

## File Organization

```
src/
├── app/               # Next.js App Router (pages, API routes)
├── components/        # React components (ui/, forms/, layouts/)
├── hooks/             # Custom React hooks
├── lib/               # Utilities, API clients, constants
├── types/             # TypeScript type definitions
└── styles/            # Global styles
```

**Naming**: `Button.tsx` (PascalCase components), `useAuth.ts` (camelCase hooks), `formatDate.ts` (camelCase utils), `market.types.ts` (types suffix)

**Limits**: 200–400 lines per file (800 max), functions under 50 lines, max 4 levels of nesting (use early returns)

## Comments

Explain WHY, not WHAT:

```typescript
// ✅ Explain non-obvious reasoning
// Use exponential backoff to avoid overwhelming the API during outages
const delay = Math.min(1000 * Math.pow(2, retryCount), 30000)

// Deliberately using mutation here for performance with large arrays
items.push(newItem)

// ❌ Stating the obvious
// Increment counter by 1
count++
```

## Avoiding Code Smells

### Long Functions

```typescript
// ❌ BAD: Function > 50 lines
function processMarketData() {
  // 100 lines of code
}

// ✅ GOOD: Split into smaller functions
function processMarketData() {
  const validated = validateData()
  const transformed = transformData(validated)
  return saveData(transformed)
}
```

### Deep Nesting

```typescript
// ❌ BAD: 5+ levels of nesting
if (user) {
  if (user.isAdmin) {
    if (market) {
      if (market.isActive) {
        if (hasPermission) {
          // Do something
        }
      }
    }
  }
}

// ✅ GOOD: Early returns
if (!user) return
if (!user.isAdmin) return
if (!market) return
if (!market.isActive) return
if (!hasPermission) return

// Do something
```

### Magic Numbers

```typescript
// ❌ BAD: Unexplained numbers
if (retryCount > 3) { }
setTimeout(callback, 500)

// ✅ GOOD: Named constants
const MAX_RETRIES = 3
const DEBOUNCE_DELAY_MS = 500

if (retryCount > MAX_RETRIES) { }
setTimeout(callback, DEBOUNCE_DELAY_MS)
```

## Code Quality Checklist

Before marking work complete, verify:

- [ ] No `any` types — proper interfaces defined
- [ ] No direct mutation — immutable patterns used
- [ ] No magic numbers — named constants used
- [ ] No deep nesting (>4 levels) — early returns applied
- [ ] Functions under 50 lines, files under 800 lines
- [ ] Error handling at every async boundary
- [ ] Comments explain WHY, never WHAT

## Validation

```bash
npx tsc --noEmit                         # Type check
npx nx lint <project-name>               # Lint specific project
npx nx run-many --target=lint --all      # Lint all projects
```

**Remember**: Code quality is not negotiable. Clear, maintainable code enables rapid development and confident refactoring.
