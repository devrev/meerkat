import { sqlToMeerkat, DecomposeResult } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import { MeerkatQueryFilter } from '@devrev/meerkat-core';

const getQueryOutput = (query: string) =>
  duckdbExec<Record<string, string>[]>(query);

// ─── FIXTURES ──────────────────────────────────────────────────────────────────

const SETUP_SQL = `
CREATE OR REPLACE TABLE tickets (
    id INTEGER,
    title VARCHAR,
    status VARCHAR,
    priority INTEGER,
    owner VARCHAR,
    category VARCHAR,
    created_date TIMESTAMP,
    resolved_at TIMESTAMP,
    amount DOUBLE,
    tags VARCHAR[]
);

INSERT INTO tickets VALUES
(1, 'Login broken', 'open', 5, 'alice', 'bug', '2024-01-15', NULL, 100.0, ['auth', 'critical']),
(2, 'Slow query', 'open', 3, 'bob', 'perf', '2024-02-01', NULL, 50.0, ['db']),
(3, 'UI glitch', 'closed', 2, 'alice', 'bug', '2024-02-10', '2024-02-15', 25.0, ['frontend']),
(4, 'Data loss', 'open', 5, 'charlie', 'bug', '2024-03-01', NULL, 200.0, ['critical', 'db']),
(5, 'Typo in docs', 'closed', 1, 'bob', 'docs', '2024-03-15', '2024-03-16', 10.0, []),
(6, 'API timeout', 'review', 4, 'alice', 'perf', '2024-04-01', NULL, 75.0, ['api']),
(7, 'Memory leak', 'open', 5, 'charlie', 'bug', '2024-04-10', NULL, 150.0, ['critical']),
(8, 'CSS broken', 'closed', 2, 'bob', 'bug', '2024-05-01', '2024-05-03', 30.0, ['frontend']),
(9, 'Auth issue', 'open', 4, 'alice', 'bug', '2024-05-15', NULL, 80.0, ['auth']),
(10, 'Perf regression', 'review', 3, 'charlie', 'perf', '2024-06-01', NULL, 60.0, ['api', 'db']);

CREATE OR REPLACE TABLE users (
    id INTEGER,
    name VARCHAR,
    team VARCHAR
);

INSERT INTO users VALUES
(1, 'alice', 'backend'),
(2, 'bob', 'frontend'),
(3, 'charlie', 'platform');
`;

// ─── HELPERS ───────────────────────────────────────────────────────────────────

function flattenFilters(filters: MeerkatQueryFilter[]): any[] {
  const result: any[] = [];
  for (const f of filters) {
    if ('and' in f) {
      result.push(...flattenFilters((f as any).and));
    } else if ('or' in f) {
      result.push(...flattenFilters((f as any).or));
    } else {
      result.push(f);
    }
  }
  return result;
}

async function decomposeAndVerifyRowCount(sql: string) {
  const result = await sqlToMeerkat({ sql, getQueryOutput });
  expect(result.success).toBe(true);
  const { tableSchema, query } = result as DecomposeResult;

  const rebuiltSql = await cubeQueryToSQL({ query, tableSchemas: [tableSchema] });
  const originalRows = await duckdbExec<any[]>(sql);
  const rebuiltRows = await duckdbExec<any[]>(rebuiltSql);

  expect(rebuiltRows.length).toBe(originalRows.length);
  return result as DecomposeResult;
}

// ─── TESTS ─────────────────────────────────────────────────────────────────────

describe('sqlToMeerkat E2E', () => {
  beforeAll(async () => {
    await duckdbExec(SETUP_SQL);
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // AGGREGATE FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('aggregate functions', () => {
    it('COUNT(*)', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status'
      );
    });

    it('SUM', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, SUM(amount) as total FROM tickets GROUP BY status'
      );
    });

    it('AVG', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, AVG(amount) as avg_amt FROM tickets GROUP BY status'
      );
    });

    it('MIN and MAX', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, MIN(amount) as min_amt, MAX(amount) as max_amt FROM tickets GROUP BY status'
      );
    });

    it('COUNT(DISTINCT col)', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(DISTINCT owner) as unique_owners FROM tickets GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBe(1);
    });

    it('multiple aggregates on same column', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, SUM(amount) as total, AVG(amount) as avg_amt, MIN(amount) as lo, MAX(amount) as hi FROM tickets GROUP BY status'
      );
    });

    it('aggregate with CASE WHEN inside', async () => {
      await decomposeAndVerifyRowCount(
        "SELECT status, SUM(CASE WHEN priority > 3 THEN amount ELSE 0 END) as high_priority_total FROM tickets GROUP BY status"
      );
    });

    it('aggregate with COALESCE inside', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, SUM(COALESCE(amount, 0)) as total FROM tickets GROUP BY status'
      );
    });

    it('aggregate with FILTER clause', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT status, COUNT(*) FILTER (WHERE priority > 3) as high_cnt FROM tickets GROUP BY status",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBeGreaterThan(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // GROUP BY VARIATIONS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('GROUP BY variations', () => {
    it('single column GROUP BY', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status'
      );
    });

    it('multi-column GROUP BY', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, priority, COUNT(*) as cnt FROM tickets GROUP BY status, priority'
      );
    });

    it('GROUP BY with expression (DATE_TRUNC)', async () => {
      await decomposeAndVerifyRowCount(
        "SELECT DATE_TRUNC('month', created_date) as month, COUNT(*) as cnt FROM tickets GROUP BY DATE_TRUNC('month', created_date)"
      );
    });

    it('GROUP BY with expression (EXTRACT)', async () => {
      await decomposeAndVerifyRowCount(
        "SELECT EXTRACT(YEAR FROM created_date) as yr, COUNT(*) as cnt FROM tickets GROUP BY EXTRACT(YEAR FROM created_date)"
      );
    });

    it('GROUP BY with CASE expression', async () => {
      await decomposeAndVerifyRowCount(
        "SELECT CASE WHEN priority >= 4 THEN 'high' ELSE 'low' END as severity, COUNT(*) as cnt FROM tickets GROUP BY CASE WHEN priority >= 4 THEN 'high' ELSE 'low' END"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // WHERE CLAUSE FILTER EXTRACTION
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('WHERE clause extraction', () => {
    it('equality: col = literal string', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE owner = 'alice' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.member === 'tickets.owner' && f.operator === 'equals')).toBe(true);
    });

    it('equality: col = integer', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority = 5 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.member === 'tickets.priority' && f.operator === 'equals')).toBe(true);
    });

    it('not equals: col != literal', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status != 'closed' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'notEquals')).toBe(true);
    });

    it('greater than: col > value', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 3 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'gt')).toBe(true);
    });

    it('greater than or equal: col >= value', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority >= 4 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'gte')).toBe(true);
    });

    it('less than: col < value', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount < 50 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'lt')).toBe(true);
    });

    it('less than or equal: col <= value', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount <= 50 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'lte')).toBe(true);
    });

    it('IS NULL', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE resolved_at IS NULL GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'notSet')).toBe(true);
    });

    it('IS NOT NULL', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE resolved_at IS NOT NULL GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'set')).toBe(true);
    });

    it('BETWEEN', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date BETWEEN '2024-02-01' AND '2024-04-01' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.some((f) => f.operator === 'inDateRange')).toBe(true);
    });

    it('multiple AND conditions', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE priority >= 4 AND owner = 'alice' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters.length).toBeGreaterThanOrEqual(2);
    });

    it('timestamp comparison', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date > '2024-03-01' GROUP BY status"
      );
      expect(query.filters).toBeDefined();
    });

    it('OR condition extracted as LogicalOrFilter', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status = 'open' OR status = 'review' GROUP BY status"
      );
      expect(query.filters).toBeDefined();
      // Should contain an OR filter structure
      const topFilter = query.filters![0];
      expect('or' in topFilter).toBe(true);
    });

    it('OR with AND branches', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE (status = 'open' AND priority > 3) OR (status = 'closed' AND priority <= 2) GROUP BY status"
      );
      expect(query.filters).toBeDefined();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // HAVING CLAUSE
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('HAVING clause', () => {
    it('HAVING with aggregate > value', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status HAVING COUNT(*) > 1'
      );
    });

    it('HAVING with aggregate >= value', async () => {
      await decomposeAndVerifyRowCount(
        'SELECT status, SUM(amount) as total FROM tickets GROUP BY status HAVING SUM(amount) >= 100'
      );
    });

    it('HAVING + WHERE combined', async () => {
      await decomposeAndVerifyRowCount(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 2 GROUP BY status HAVING COUNT(*) >= 2"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // ORDER BY
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('ORDER BY', () => {
    it('ORDER BY aggregate DESC', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC'
      );
      expect(query.order).toBeDefined();
      const orderValues = Object.values(query.order!);
      expect(orderValues).toContain('desc');
    });

    it('ORDER BY dimension ASC', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY status ASC'
      );
      expect(query.order).toBeDefined();
    });

    it('ORDER BY multiple columns', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, priority, COUNT(*) as cnt FROM tickets GROUP BY status, priority ORDER BY status ASC, cnt DESC'
      );
      expect(Object.keys(query.order!).length).toBe(2);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // LIMIT / OFFSET
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('LIMIT and OFFSET', () => {
    it('LIMIT only', async () => {
      const { query } = await decomposeAndVerifyRowCount(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC LIMIT 2'
      );
      expect(query.limit).toBe(2);
    });

    it('LIMIT + OFFSET', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC LIMIT 2 OFFSET 1',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { query } = result as DecomposeResult;
      expect(query.limit).toBe(2);
      expect(query.offset).toBe(1);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // CTEs (Common Table Expressions)
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('CTEs', () => {
    it('single CTE', async () => {
      const result = await sqlToMeerkat({
        sql: `
          WITH recent AS (SELECT * FROM tickets WHERE created_date > '2024-03-01')
          SELECT status, COUNT(*) as cnt FROM recent GROUP BY status
        `,
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBeGreaterThan(0);
    });

    it('multiple CTEs', async () => {
      const result = await sqlToMeerkat({
        sql: `
          WITH
            high_priority AS (SELECT * FROM tickets WHERE priority >= 4),
            open_tickets AS (SELECT * FROM high_priority WHERE status = 'open')
          SELECT owner, COUNT(*) as cnt FROM open_tickets GROUP BY owner
        `,
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBeGreaterThan(0);
    });

    it('rejects WITH RECURSIVE', async () => {
      const result = await sqlToMeerkat({
        sql: `
          WITH RECURSIVE cnt(x) AS (
            SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5
          )
          SELECT * FROM cnt
        `,
        getQueryOutput,
      });
      expect(result.success).toBe(false);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // SET OPERATIONS (BLOCKERS)
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('set operations (blockers)', () => {
    it('rejects UNION ALL', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT status, COUNT(*) as cnt FROM tickets WHERE status = 'open' GROUP BY status UNION ALL SELECT status, COUNT(*) FROM tickets WHERE status = 'closed' GROUP BY status",
        getQueryOutput,
      });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.reason).toContain('UNION');
    });

    it('rejects EXCEPT', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT id FROM tickets EXCEPT SELECT id FROM tickets WHERE priority < 3',
        getQueryOutput,
      });
      expect(result.success).toBe(false);
    });

    it('rejects INTERSECT', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT owner FROM tickets WHERE status = 'open' INTERSECT SELECT owner FROM tickets WHERE priority >= 4",
        getQueryOutput,
      });
      expect(result.success).toBe(false);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // WINDOW FUNCTIONS (BEST EFFORT)
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('window functions (best-effort skip)', () => {
    it('ROW_NUMBER alongside aggregate — extracts aggregate, skips window', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(*) as cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn FROM tickets GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema, warnings } = result as DecomposeResult;
      expect(tableSchema.measures.some((m) => m.name === 'cnt')).toBe(true);
      expect(warnings.some((w) => w.toLowerCase().includes('window'))).toBe(true);
    });

    it('RANK alongside aggregate', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, SUM(amount) as total, RANK() OVER (ORDER BY SUM(amount) DESC) as rnk FROM tickets GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.some((m) => m.name === 'total')).toBe(true);
    });

    it('only window functions — produces dimensions from non-window items', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT id, status, ROW_NUMBER() OVER (ORDER BY id) as rn FROM tickets',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.dimensions.length).toBeGreaterThan(0);
      expect(tableSchema.measures).toEqual([]);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // NON-AGGREGATED QUERIES
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('non-aggregated queries', () => {
    it('plain SELECT columns', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT id, title, status, priority FROM tickets',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema, query } = result as DecomposeResult;
      expect(tableSchema.measures).toEqual([]);
      expect(tableSchema.dimensions.length).toBe(4);
      expect(query.measures).toEqual([]);
    });

    it('SELECT * (star expression)', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT * FROM tickets',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('SELECT with WHERE but no aggregation', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT id, title, status FROM tickets WHERE owner = 'alice' AND priority > 3",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { query } = result as DecomposeResult;
      expect(query.filters).toBeDefined();
    });

    it('SELECT with computed columns (no aggregation)', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT id, title, amount * 1.1 as amount_with_tax FROM tickets",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.dimensions.length).toBe(3);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // SUBQUERIES
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('subqueries', () => {
    it('subquery in FROM clause', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, cnt FROM (SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status) sub WHERE cnt > 1',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('aggregation over subquery', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT owner, SUM(cnt) as total FROM (SELECT owner, COUNT(*) as cnt FROM tickets GROUP BY owner, status) sub GROUP BY owner",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBeGreaterThan(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // STRING AND SPECIAL VALUES
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('string and special value handling', () => {
    it('string with single quote (escaped)', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT status, COUNT(*) as cnt FROM tickets WHERE title = 'it''s broken' GROUP BY status",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('numeric zero as filter value', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount > 0 GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { query } = result as DecomposeResult;
      expect(query.filters).toBeDefined();
    });

    it('negative number in filter', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount > -10 GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // JOINS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('JOINs', () => {
    it('INNER JOIN with aggregation', async () => {
      const result = await sqlToMeerkat({
        sql: `
          SELECT u.team, COUNT(*) as cnt
          FROM tickets t
          JOIN users u ON t.owner = u.name
          GROUP BY u.team
        `,
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBeGreaterThan(0);
      expect(tableSchema.dimensions.length).toBeGreaterThan(0);
    });

    it('LEFT JOIN with aggregation', async () => {
      const result = await sqlToMeerkat({
        sql: `
          SELECT u.team, COUNT(t.id) as ticket_count
          FROM users u
          LEFT JOIN tickets t ON u.name = t.owner
          GROUP BY u.team
        `,
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // CASTING AND TYPE FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('casting and type functions', () => {
    it('CAST in SELECT', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, CAST(AVG(amount) AS INTEGER) as avg_int FROM tickets GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('::type notation', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, (SUM(amount))::INTEGER as total_int FROM tickets GROUP BY status',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // DUCKDB-SPECIFIC SYNTAX
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('DuckDB-specific syntax', () => {
    it('QUALIFY clause (treated as pass-through)', async () => {
      const result = await sqlToMeerkat({
        sql: `
          SELECT id, status, owner,
                 ROW_NUMBER() OVER (PARTITION BY status ORDER BY created_date DESC) as rn
          FROM tickets
          QUALIFY rn = 1
        `,
        getQueryOutput,
      });
      // Should succeed — window items skipped, QUALIFY in AST but not blocking
      expect(result.success).toBe(true);
    });

    it('DISTINCT ON', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT DISTINCT ON (status) status, title, priority FROM tickets ORDER BY status, priority DESC',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('string_agg aggregate', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT status, STRING_AGG(title, ', ') as titles FROM tickets GROUP BY status",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { tableSchema } = result as DecomposeResult;
      expect(tableSchema.measures.length).toBe(1);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // ERROR HANDLING
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('error handling', () => {
    it('invalid SQL returns failure with reason', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELEC broken syntax!!!',
        getQueryOutput,
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.reason).toContain('DuckDB parse failed');
      }
    });

    it('empty SQL returns failure', async () => {
      const result = await sqlToMeerkat({
        sql: '',
        getQueryOutput,
      });
      expect(result.success).toBe(false);
    });
  });
});
