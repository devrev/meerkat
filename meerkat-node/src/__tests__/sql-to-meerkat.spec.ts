import {
  DecomposeResult,
  MeerkatQueryFilter,
  sqlToMeerkat,
} from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function flattenFilters(filters: MeerkatQueryFilter[]): any[] {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result: any[] = [];
  for (const f of filters) {
    if ('and' in f) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      result.push(...flattenFilters((f as any).and));
    } else if ('or' in f) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      result.push(...flattenFilters((f as any).or));
    } else {
      result.push(f);
    }
  }
  return result;
}

async function decomposeAndRebuild(sql: string) {
  const result = await sqlToMeerkat({ sql, getQueryOutput });
  expect(result.success).toBe(true);
  const { tableSchema, query, warnings } = result as DecomposeResult;
  const rebuiltSql = await cubeQueryToSQL({
    query,
    tableSchemas: [tableSchema],
  });
  return { tableSchema, query, warnings, rebuiltSql };
}

async function verifyExactRowMatch(sql: string) {
  const { rebuiltSql } = await decomposeAndRebuild(sql);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const originalRows = await duckdbExec<any[]>(sql);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rebuiltRows = await duckdbExec<any[]>(rebuiltSql);
  expect(rebuiltRows.length).toBe(originalRows.length);
}

// ─── TESTS ─────────────────────────────────────────────────────────────────────

describe('sqlToMeerkat E2E', () => {
  beforeAll(async () => {
    await duckdbExec(SETUP_SQL);
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // AGGREGATE FUNCTIONS — exact SQL assertions
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('aggregate functions', () => {
    it('COUNT(*)', async () => {
      const { rebuiltSql, tableSchema, query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toEqual([
        { name: 'cnt', sql: 'count_star()', type: 'number' },
      ]);
      expect(tableSchema.dimensions).toEqual([
        { name: 'status', sql: 'status', type: 'string' },
      ]);
      expect(query.measures).toEqual(['tickets.cnt']);
      expect(query.dimensions).toEqual(['tickets.status']);
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets GROUP BY tickets__status"
      );
    });

    it('SUM', async () => {
      const { rebuiltSql, tableSchema } = await decomposeAndRebuild(
        'SELECT status, SUM(amount) as total FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toEqual([
        { name: 'total', sql: 'sum(amount)', type: 'number' },
      ]);
      expect(rebuiltSql).toBe(
        "SELECT sum(amount) AS tickets__total ,   tickets__status FROM (SELECT status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets GROUP BY tickets__status"
      );
    });

    it('AVG', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT status, AVG(amount) as avg_amt FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toEqual([
        { name: 'avg_amt', sql: 'avg(amount)', type: 'number' },
      ]);
    });

    it('MIN and MAX', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT status, MIN(amount) as min_amt, MAX(amount) as max_amt FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toEqual([
        { name: 'min_amt', sql: 'min(amount)', type: 'number' },
        { name: 'max_amt', sql: 'max(amount)', type: 'number' },
      ]);
    });

    it('COUNT(DISTINCT col)', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT status, COUNT(DISTINCT owner) as unique_owners FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures.length).toBe(1);
      expect(tableSchema.measures[0].name).toBe('unique_owners');
    });

    it('multiple aggregates on same column', async () => {
      await verifyExactRowMatch(
        'SELECT status, SUM(amount) as total, AVG(amount) as avg_amt, MIN(amount) as lo, MAX(amount) as hi FROM tickets GROUP BY status'
      );
    });

    it('aggregate with CASE WHEN inside', async () => {
      await verifyExactRowMatch(
        'SELECT status, SUM(CASE WHEN priority > 3 THEN amount ELSE 0 END) as high_priority_total FROM tickets GROUP BY status'
      );
    });

    it('aggregate with COALESCE inside', async () => {
      await verifyExactRowMatch(
        'SELECT status, SUM(COALESCE(amount, 0)) as total FROM tickets GROUP BY status'
      );
    });

    it('aggregate with FILTER clause', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT status, COUNT(*) FILTER (WHERE priority > 3) as high_cnt FROM tickets GROUP BY status',
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
      await verifyExactRowMatch(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status'
      );
    });

    it('multi-column GROUP BY', async () => {
      await verifyExactRowMatch(
        'SELECT status, priority, COUNT(*) as cnt FROM tickets GROUP BY status, priority'
      );
    });

    it('GROUP BY with expression (DATE_TRUNC)', async () => {
      await verifyExactRowMatch(
        "SELECT DATE_TRUNC('month', created_date) as month, COUNT(*) as cnt FROM tickets GROUP BY DATE_TRUNC('month', created_date)"
      );
    });

    it('GROUP BY with expression (EXTRACT)', async () => {
      await verifyExactRowMatch(
        'SELECT EXTRACT(YEAR FROM created_date) as yr, COUNT(*) as cnt FROM tickets GROUP BY EXTRACT(YEAR FROM created_date)'
      );
    });

    it('GROUP BY with CASE expression', async () => {
      await verifyExactRowMatch(
        "SELECT CASE WHEN priority >= 4 THEN 'high' ELSE 'low' END as severity, COUNT(*) as cnt FROM tickets GROUP BY CASE WHEN priority >= 4 THEN 'high' ELSE 'low' END"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // WHERE CLAUSE FILTER EXTRACTION — exact filter structure
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('WHERE clause extraction', () => {
    it('equality: col = literal string', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE owner = 'alice' GROUP BY status"
      );
      expect(query.filters).toEqual([
        { member: 'tickets.owner', operator: 'equals', values: ['alice'] },
      ]);
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT owner AS tickets__owner, status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets WHERE (tickets__owner = 'alice') GROUP BY tickets__status"
      );
    });

    it('equality: col = integer', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority = 5 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'equals', values: ['5'] },
      ]);
    });

    it('not equals: col != literal', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status != 'closed' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.status', operator: 'notEquals', values: ['closed'] }
      );
    });

    it('greater than: col > value', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 3 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'gt', values: ['3'] },
      ]);
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT priority AS tickets__priority, status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets WHERE (tickets__priority > 3) GROUP BY tickets__status"
      );
    });

    it('greater than or equal: col >= value', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority >= 4 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'gte', values: ['4'] },
      ]);
    });

    it('less than: col < value', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount < 50 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.amount', operator: 'lt', values: ['50'] },
      ]);
    });

    it('less than or equal: col <= value', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount <= 50 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.amount', operator: 'lte', values: ['50'] },
      ]);
    });

    it('IS NULL', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE resolved_at IS NULL GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.resolved_at', operator: 'notSet', values: [] },
      ]);
    });

    it('IS NOT NULL', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE resolved_at IS NOT NULL GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.resolved_at', operator: 'set', values: [] },
      ]);
    });

    it('BETWEEN date column with string literals emits gte/lte pair', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date BETWEEN '2024-02-01' AND '2024-04-01' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.created_date', operator: 'gte', values: ['2024-02-01'] }
      );
      expect(filters).toContainEqual(
        { member: 'tickets.created_date', operator: 'lte', values: ['2024-04-01'] }
      );
    });

    it('multiple AND conditions', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE priority >= 4 AND owner = 'alice' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.priority', operator: 'gte', values: ['4'] }
      );
      expect(filters).toContainEqual(
        { member: 'tickets.owner', operator: 'equals', values: ['alice'] }
      );
    });

    it('timestamp comparison', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date > '2024-03-01' GROUP BY status"
      );
      expect(query.filters).toBeDefined();
      const filters = flattenFilters(query.filters!);
      expect(filters[0].operator).toBe('gt');
    });

    it('reversed comparison: literal on left (5 = col)', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE 5 = priority GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'equals', values: ['5'] },
      ]);
    });

    it('reversed comparison: literal < col becomes col > literal', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE 3 < priority GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'gt', values: ['3'] },
      ]);
    });

    it('OR condition extracted as LogicalOrFilter', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status = 'open' OR status = 'review' GROUP BY status"
      );
      const topFilter = query.filters![0];
      expect('or' in topFilter).toBe(true);
    });

    it('OR with AND branches', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE (status = 'open' AND priority > 3) OR (status = 'closed' AND priority <= 2) GROUP BY status"
      );
      expect(query.filters).toBeDefined();
      await verifyExactRowMatch(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE (status = 'open' AND priority > 3) OR (status = 'closed' AND priority <= 2) GROUP BY status"
      );
    });

    it('AND with nested OR: extracts both parts', async () => {
      const { query, warnings } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 3 AND (status = 'open' OR status = 'closed') GROUP BY status"
      );
      expect(warnings).toEqual([]);
      const topFilter = query.filters![0];
      expect('and' in topFilter).toBe(true);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const andMembers = (topFilter as any).and;
      expect(andMembers).toContainEqual({
        member: 'tickets.priority',
        operator: 'gt',
        values: ['3'],
      });
      const orMember = andMembers.find((f: unknown) => f && typeof f === 'object' && 'or' in f);
      expect(orMember).toBeDefined();
      expect(orMember.or).toContainEqual({
        member: 'tickets.status',
        operator: 'equals',
        values: ['open'],
      });
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // HAVING CLAUSE
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('HAVING clause', () => {
    it('HAVING with aggregate > value', async () => {
      await verifyExactRowMatch(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status HAVING COUNT(*) > 1'
      );
    });

    it('HAVING with aggregate >= value', async () => {
      await verifyExactRowMatch(
        'SELECT status, SUM(amount) as total FROM tickets GROUP BY status HAVING SUM(amount) >= 100'
      );
    });

    it('HAVING + WHERE combined', async () => {
      await verifyExactRowMatch(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 2 GROUP BY status HAVING COUNT(*) >= 2'
      );
    });

    it('reversed HAVING: literal < aggregate', async () => {
      await verifyExactRowMatch(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status HAVING 1 < COUNT(*)'
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // ORDER BY — exact order structure
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('ORDER BY', () => {
    it('ORDER BY aggregate DESC', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC'
      );
      expect(query.order).toEqual({ 'tickets.cnt': 'desc' });
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets GROUP BY tickets__status ORDER BY tickets__cnt DESC"
      );
    });

    it('ORDER BY dimension ASC', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY status ASC'
      );
      expect(query.order).toEqual({ 'tickets.status': 'asc' });
    });

    it('ORDER BY multiple columns', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, priority, COUNT(*) as cnt FROM tickets GROUP BY status, priority ORDER BY status ASC, cnt DESC'
      );
      expect(query.order).toEqual({
        'tickets.status': 'asc',
        'tickets.cnt': 'desc',
      });
    });

    it('ORDER BY positional reference (ORDER BY 2 DESC)', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY 2 DESC'
      );
      expect(query.order).toEqual({ 'tickets.cnt': 'desc' });
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // LIMIT / OFFSET — exact values
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('LIMIT and OFFSET', () => {
    it('LIMIT only', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC LIMIT 2'
      );
      expect(query.limit).toBe(2);
      expect(query.offset).toBeUndefined();
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets GROUP BY tickets__status ORDER BY tickets__cnt DESC LIMIT 2"
      );
    });

    it('LIMIT + OFFSET', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets GROUP BY status ORDER BY cnt DESC LIMIT 2 OFFSET 1'
      );
      expect(query.limit).toBe(2);
      expect(query.offset).toBe(1);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // CTEs
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('CTEs', () => {
    it('single CTE', async () => {
      const { tableSchema } = await decomposeAndRebuild(`
        WITH recent AS (SELECT * FROM tickets WHERE created_date > '2024-03-01')
        SELECT status, COUNT(*) as cnt FROM recent GROUP BY status
      `);
      expect(tableSchema.measures).toEqual([
        { name: 'cnt', sql: 'count_star()', type: 'number' },
      ]);
    });

    it('multiple CTEs', async () => {
      const { tableSchema } = await decomposeAndRebuild(`
        WITH
          high_priority AS (SELECT * FROM tickets WHERE priority >= 4),
          open_tickets AS (SELECT * FROM high_priority WHERE status = 'open')
        SELECT owner, COUNT(*) as cnt FROM open_tickets GROUP BY owner
      `);
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
      if (!result.success) {
        expect(result.reason).toBe('WITH RECURSIVE not supported');
      }
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
      if (!result.success) {
        expect(result.reason).toBe('UNION/INTERSECT/EXCEPT not supported');
      }
    });

    it('rejects EXCEPT', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT id FROM tickets EXCEPT SELECT id FROM tickets WHERE priority < 3',
        getQueryOutput,
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.reason).toBe('UNION/INTERSECT/EXCEPT not supported');
      }
    });

    it('rejects INTERSECT', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT owner FROM tickets WHERE status = 'open' INTERSECT SELECT owner FROM tickets WHERE priority >= 4",
        getQueryOutput,
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.reason).toBe('UNION/INTERSECT/EXCEPT not supported');
      }
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // WINDOW FUNCTIONS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('window functions (best-effort skip)', () => {
    it('ROW_NUMBER alongside aggregate — extracts aggregate, skips window', async () => {
      const { tableSchema, warnings } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toContainEqual(
        { name: 'cnt', sql: 'count_star()', type: 'number' }
      );
      expect(warnings.some((w) => w.toLowerCase().includes('window'))).toBe(true);
    });

    it('RANK alongside aggregate', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT status, SUM(amount) as total, RANK() OVER (ORDER BY SUM(amount) DESC) as rnk FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures).toContainEqual(
        { name: 'total', sql: 'sum(amount)', type: 'number' }
      );
    });

    it('only window functions — produces dimensions from non-window items', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT id, status, ROW_NUMBER() OVER (ORDER BY id) as rn FROM tickets'
      );
      expect(tableSchema.dimensions.length).toBeGreaterThan(0);
      expect(tableSchema.measures).toEqual([]);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // NON-AGGREGATED QUERIES — exact SQL
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('non-aggregated queries', () => {
    it('plain SELECT columns', async () => {
      const { tableSchema, query, rebuiltSql } = await decomposeAndRebuild(
        'SELECT id, title, status, priority FROM tickets'
      );
      expect(tableSchema.measures).toEqual([]);
      expect(tableSchema.dimensions).toEqual([
        { name: 'id', sql: 'id', type: 'string' },
        { name: 'title', sql: 'title', type: 'string' },
        { name: 'status', sql: 'status', type: 'string' },
        { name: 'priority', sql: 'priority', type: 'string' },
      ]);
      expect(query.measures).toEqual([]);
      expect(rebuiltSql).toBe(
        "SELECT  tickets__id,  tickets__title,  tickets__status,  tickets__priority FROM (SELECT id AS tickets__id, title AS tickets__title, status AS tickets__status, priority AS tickets__priority, * FROM (SELECT * FROM tickets) AS tickets) AS tickets"
      );
    });

    it('SELECT * (star expression)', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT * FROM tickets',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
    });

    it('SELECT with WHERE but no aggregation', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT id, title, status FROM tickets WHERE owner = 'alice' AND priority > 3"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.owner', operator: 'equals', values: ['alice'] }
      );
      expect(filters).toContainEqual(
        { member: 'tickets.priority', operator: 'gt', values: ['3'] }
      );
    });

    it('SELECT with computed columns (no aggregation)', async () => {
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT id, title, amount * 1.1 as amount_with_tax FROM tickets'
      );
      expect(tableSchema.dimensions.length).toBe(3);
      expect(tableSchema.dimensions[2].name).toBe('amount_with_tax');
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
      const { tableSchema } = await decomposeAndRebuild(
        'SELECT owner, SUM(cnt) as total FROM (SELECT owner, COUNT(*) as cnt FROM tickets GROUP BY owner, status) sub GROUP BY owner'
      );
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
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount > 0 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.amount', operator: 'gt', values: ['0'] },
      ]);
    });

    it('decimal value in filter', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount > 99.5 GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.amount', operator: 'gt', values: ['99.5'] },
      ]);
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
      const { tableSchema } = await decomposeAndRebuild(`
        SELECT u.team, COUNT(*) as cnt
        FROM tickets t
        JOIN users u ON t.owner = u.name
        GROUP BY u.team
      `);
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

    it('WHERE on joined table retained in base SQL, primary table extracted', async () => {
      const { query, tableSchema, warnings } = await decomposeAndRebuild(`
        SELECT t.status, COUNT(*) as cnt
        FROM tickets t
        JOIN users u ON t.owner = u.name
        WHERE u.team = 'backend' AND t.priority > 3
        GROUP BY t.status
      `);
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual({
        member: 't.priority',
        operator: 'gt',
        values: ['3'],
      });
      expect(filters).not.toContainEqual(
        expect.objectContaining({ member: 'u.team' })
      );
      expect(tableSchema.sql).toContain("u.team = 'backend'");
      expect(warnings).toContain(
        'Non-extractable WHERE condition retained in base SQL'
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // CASTING
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
  // DUCKDB-SPECIFIC
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
      const { tableSchema } = await decomposeAndRebuild(
        "SELECT status, STRING_AGG(title, ', ') as titles FROM tickets GROUP BY status"
      );
      expect(tableSchema.measures.length).toBe(1);
      expect(tableSchema.measures[0].name).toBe('titles');
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // IN / NOT IN — exact filter structure
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('IN / NOT IN extraction', () => {
    it('IN list extracted as in operator', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status IN ('open', 'review') GROUP BY status"
      );
      expect(query.filters).toEqual([
        { member: 'tickets.status', operator: 'in', values: ['open', 'review'] },
      ]);
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets WHERE (tickets__status IN ('open', 'review')) GROUP BY tickets__status"
      );
    });

    it('NOT IN extracted as notIn operator', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status NOT IN ('closed') GROUP BY status"
      );
      expect(query.filters).toEqual([
        { member: 'tickets.status', operator: 'notIn', values: ['closed'] },
      ]);
    });

    it('IN with integers', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE priority IN (3, 4, 5) GROUP BY status'
      );
      expect(query.filters).toEqual([
        { member: 'tickets.priority', operator: 'in', values: ['3', '4', '5'] },
      ]);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // LIKE — exact filter structure
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('LIKE extraction', () => {
    it('ILIKE %value% extracted as contains', async () => {
      const { query, rebuiltSql } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE title ILIKE '%broken%' GROUP BY status"
      );
      expect(query.filters).toEqual([
        { member: 'tickets.title', operator: 'contains', values: ['broken'] },
      ]);
      expect(rebuiltSql).toBe(
        "SELECT count_star() AS tickets__cnt ,   tickets__status FROM (SELECT title AS tickets__title, status AS tickets__status, * FROM (SELECT * FROM tickets) AS tickets) AS tickets WHERE (tickets__title ~~* '%broken%') GROUP BY tickets__status"
      );
    });

    it('NOT ILIKE %value% extracted as notContains', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE title NOT ILIKE '%broken%' GROUP BY status"
      );
      expect(query.filters).toEqual([
        { member: 'tickets.title', operator: 'notContains', values: ['broken'] },
      ]);
    });

    it('complex LIKE pattern kept in base SQL (not extracted)', async () => {
      const result = await sqlToMeerkat({
        sql: "SELECT status, COUNT(*) as cnt FROM tickets WHERE title LIKE '%br_ken%' GROUP BY status",
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      const { warnings } = result as DecomposeResult;
      expect(warnings).toContainEqual('Non-extractable WHERE condition retained in base SQL');
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // RESIDUAL WHERE RETENTION
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('residual WHERE retention', () => {
    it('non-extractable condition retained — row count still matches', async () => {
      await verifyExactRowMatch(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE title LIKE '%br_ken%' GROUP BY status"
      );
    });

    it('mix of extractable and non-extractable conditions', async () => {
      await verifyExactRowMatch(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE priority > 3 AND title LIKE 'Login%' GROUP BY status"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // BETWEEN SEMANTICS
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('BETWEEN semantics', () => {
    it('numeric BETWEEN emits gte + lte pair', async () => {
      const { query } = await decomposeAndRebuild(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount BETWEEN 50 AND 150 GROUP BY status'
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.amount', operator: 'gte', values: ['50'] }
      );
      expect(filters).toContainEqual(
        { member: 'tickets.amount', operator: 'lte', values: ['150'] }
      );
    });

    it('numeric BETWEEN row count matches original', async () => {
      await verifyExactRowMatch(
        'SELECT status, COUNT(*) as cnt FROM tickets WHERE amount BETWEEN 50 AND 150 GROUP BY status'
      );
    });

    it('date BETWEEN with string literals emits gte/lte pair', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date BETWEEN '2024-01-01' AND '2024-06-01' GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual(
        { member: 'tickets.created_date', operator: 'gte', values: ['2024-01-01'] }
      );
      expect(filters).toContainEqual(
        { member: 'tickets.created_date', operator: 'lte', values: ['2024-06-01'] }
      );
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

    it('SELECT without FROM succeeds with query table name', async () => {
      const result = await sqlToMeerkat({
        sql: 'SELECT 1 + 1 as result',
        getQueryOutput,
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.tableSchema.name).toBe('query');
        expect(result.tableSchema.dimensions.length).toBe(1);
      }
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // CAST/TIMESTAMPTZ FILTER EXTRACTION
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('CAST and typed constant filters', () => {
    it('CAST comparison extracted as filter', async () => {
      const { query } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date >= CAST('2024-03-01' AS TIMESTAMP) GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual({
        member: 'tickets.created_date',
        operator: 'gte',
        values: ['2024-03-01'],
      });
    });

    it('multiple CAST filters all extracted', async () => {
      const { query, warnings } = await decomposeAndRebuild(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date >= CAST('2024-01-01' AS TIMESTAMP) AND created_date < CAST('2024-07-01' AS TIMESTAMP) GROUP BY status"
      );
      const filters = flattenFilters(query.filters!);
      expect(filters).toContainEqual({
        member: 'tickets.created_date',
        operator: 'gte',
        values: ['2024-01-01'],
      });
      expect(filters).toContainEqual({
        member: 'tickets.created_date',
        operator: 'lt',
        values: ['2024-07-01'],
      });
      expect(warnings).toEqual([]);
    });

    it('CAST filter round-trip produces correct row count', async () => {
      await verifyExactRowMatch(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE created_date >= CAST('2024-03-01' AS TIMESTAMP) GROUP BY status"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // ROUND-TRIP CORRECTNESS — verify decompose+rebuild matches original
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('round-trip correctness', () => {
    it('aggregate with WHERE + ORDER + LIMIT', async () => {
      await verifyExactRowMatch(
        "SELECT owner, SUM(amount) as total FROM tickets WHERE status = 'open' GROUP BY owner ORDER BY total DESC LIMIT 3"
      );
    });

    it('multi-column GROUP BY with HAVING', async () => {
      await verifyExactRowMatch(
        'SELECT status, owner, COUNT(*) as cnt FROM tickets GROUP BY status, owner HAVING COUNT(*) >= 2'
      );
    });

    it('IN filter with ORDER BY', async () => {
      await verifyExactRowMatch(
        "SELECT status, COUNT(*) as cnt FROM tickets WHERE status IN ('open', 'review') GROUP BY status ORDER BY cnt DESC"
      );
    });

    it('multiple aggregates with WHERE + HAVING + ORDER + LIMIT', async () => {
      await verifyExactRowMatch(
        "SELECT owner, COUNT(*) as cnt, SUM(amount) as total, AVG(priority) as avg_pri FROM tickets WHERE status != 'closed' GROUP BY owner HAVING COUNT(*) >= 2 ORDER BY total DESC LIMIT 5"
      );
    });

    it('non-aggregated with IN and ORDER', async () => {
      await verifyExactRowMatch(
        "SELECT id, title, owner, priority FROM tickets WHERE priority IN (4, 5) AND status = 'open' ORDER BY priority DESC, id ASC LIMIT 5"
      );
    });

    it('expression in SELECT with GROUP BY', async () => {
      await verifyExactRowMatch(
        "SELECT CASE WHEN priority >= 4 THEN 'high' WHEN priority >= 2 THEN 'medium' ELSE 'low' END as bucket, COUNT(*) as cnt, SUM(amount) as total FROM tickets GROUP BY CASE WHEN priority >= 4 THEN 'high' WHEN priority >= 2 THEN 'medium' ELSE 'low' END ORDER BY total DESC"
      );
    });

    it('arithmetic measure: SUM/COUNT ratio', async () => {
      const { tableSchema, query } = await decomposeAndRebuild(
        'SELECT status, SUM(amount) / COUNT(*) as avg_amount FROM tickets GROUP BY status'
      );
      expect(tableSchema.measures.length).toBe(1);
      expect(tableSchema.measures[0].name).toBe('avg_amount');
      expect(query.measures).toEqual(['tickets.avg_amount']);
      await verifyExactRowMatch(
        'SELECT status, SUM(amount) / COUNT(*) as avg_amount FROM tickets GROUP BY status'
      );
    });

    it('arithmetic measure with constant: SUM(amount) * 1.1', async () => {
      await verifyExactRowMatch(
        'SELECT status, SUM(amount) * 1.1 as inflated FROM tickets GROUP BY status'
      );
    });

    it('non-aggregated query ORDER BY column', async () => {
      await verifyExactRowMatch(
        'SELECT id, title, priority FROM tickets ORDER BY priority DESC, id ASC LIMIT 5'
      );
    });

    it('JOIN with mixed extractable/non-extractable filters row count', async () => {
      await verifyExactRowMatch(
        "SELECT t.status, COUNT(*) as cnt FROM tickets t JOIN users u ON t.owner = u.name WHERE t.priority > 3 AND u.team = 'backend' GROUP BY t.status"
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // PRODUCTION QUERIES — real-world SQL patterns
  // ═══════════════════════════════════════════════════════════════════════════════

  describe('production queries', () => {
    describe('FULL OUTER JOIN with subqueries', () => {
      it('decomposes into dimensions with correct table name and order', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT COALESCE(c.day, cl.day) AS day,
       COALESCE(c.tickets_created, 0) AS tickets_created,
       COALESCE(cl.tickets_closed, 0) AS tickets_closed
FROM (
  SELECT DATE_TRUNC('day', created_date) AS day, COUNT(*) AS tickets_created
  FROM devrev.ticket
  WHERE created_date >= TIMESTAMPTZ '2026-04-27T00:00:00+00:00'
  GROUP BY day
) c
FULL OUTER JOIN (
  SELECT DATE_TRUNC('day', actual_close_date) AS day, COUNT(*) AS tickets_closed
  FROM devrev.ticket
  WHERE actual_close_date >= TIMESTAMPTZ '2026-04-27T00:00:00+00:00'
  GROUP BY day
) cl ON c.day = cl.day
ORDER BY day
LIMIT 100`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query } = result as DecomposeResult;
        expect(tableSchema.name).toBe('c');
        expect(tableSchema.sql).toBe(
          `SELECT * FROM (SELECT date_trunc('day', created_date) AS "day", count_star() AS tickets_created FROM devrev.ticket WHERE (created_date >= CAST('2026-04-27T00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE)) GROUP BY "day") AS c FULL JOIN (SELECT date_trunc('day', actual_close_date) AS "day", count_star() AS tickets_closed FROM devrev.ticket WHERE (actual_close_date >= CAST('2026-04-27T00:00:00+00:00' AS TIMESTAMP WITH TIME ZONE)) GROUP BY "day") AS cl ON ((c."day" = cl."day"))`
        );
        expect(tableSchema.measures).toEqual([]);
        expect(tableSchema.dimensions.map((d) => d.name)).toEqual([
          'day',
          'tickets_created',
          'tickets_closed',
        ]);
        expect(query.filters).toBeUndefined();
        expect(query.order).toEqual({ 'c.day': 'asc' });
        expect(query.limit).toBe(100);
      });
    });

    describe('Multi-JOIN with PERCENTILE_CONT', () => {
      it('classifies ROUND(AVG(...)) and ROUND(CAST(PERCENTILE_CONT(...))) as measures', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT
  t.applies_to_part_id,
  COALESCE(f.name, cap.name, p.name) AS part_name,
  ROUND(AVG(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 60.0, 2) AS avg_resolution_hours,
  ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 60.0, 2) AS median_resolution_hours,
  COUNT(*) AS ticket_count
FROM devrev.ticket t
LEFT JOIN devrev.feature f ON t.applies_to_part_id = f.id
LEFT JOIN devrev.capability cap ON t.applies_to_part_id = cap.id
LEFT JOIN devrev.product p ON t.applies_to_part_id = p.id
WHERE t.state = 'closed'
  AND t.actual_close_date IS NOT NULL
  AND t.created_date IS NOT NULL
GROUP BY t.applies_to_part_id, COALESCE(f.name, cap.name, p.name)
ORDER BY ticket_count DESC
LIMIT 100`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query } = result as DecomposeResult;
        expect(tableSchema.name).toBe('t');
        expect(tableSchema.measures.map((m) => m.name)).toEqual([
          'avg_resolution_hours',
          'median_resolution_hours',
          'ticket_count',
        ]);
        expect(tableSchema.dimensions.map((d) => d.name)).toContain(
          'applies_to_part_id'
        );
        expect(tableSchema.dimensions.map((d) => d.name)).toContain(
          'part_name'
        );
        expect(query.order).toEqual({ 't.ticket_count': 'desc' });
        expect(query.limit).toBe(100);
      });

      it('extracts WHERE filters with correct table prefix', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT t.applies_to_part_id, COUNT(*) AS ticket_count
FROM devrev.ticket t
LEFT JOIN devrev.feature f ON t.applies_to_part_id = f.id
WHERE t.state = 'closed'
  AND t.actual_close_date IS NOT NULL
  AND t.created_date IS NOT NULL
GROUP BY t.applies_to_part_id
LIMIT 100`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query } = result as DecomposeResult;
        expect(tableSchema.sql).toBe(
          'SELECT * FROM devrev.ticket AS t LEFT JOIN devrev.feature AS f ON ((t.applies_to_part_id = f.id))'
        );
        const filters = flattenFilters(query.filters!);
        expect(filters).toContainEqual({
          member: 't.state',
          operator: 'equals',
          values: ['closed'],
        });
        expect(filters).toContainEqual({
          member: 't.actual_close_date',
          operator: 'set',
          values: [],
        });
        expect(filters).toContainEqual({
          member: 't.created_date',
          operator: 'set',
          values: [],
        });
      });
    });

    describe('CROSS JOIN UNNEST with PERCENTILE_CONT', () => {
      it('extracts filters and retains non-extractable COALESCE IS NOT NULL in residual', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT
  COALESCE(f.name, cap.name) AS part_name,
  ROUND(AVG(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 60.0, 2) AS avg_resolution_hours,
  ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 60.0, 2) AS median_resolution_hours,
  COUNT(*) AS ticket_count
FROM devrev.ticket t
CROSS JOIN UNNEST(t.ancestral_product) AS ap(product_id)
LEFT JOIN devrev.feature f ON t.applies_to_part_id = f.id
LEFT JOIN devrev.capability cap ON t.applies_to_part_id = cap.id
WHERE t.state = 'closed'
  AND t.actual_close_date IS NOT NULL
  AND t.created_date IS NOT NULL
  AND ap.product_id = 'don:core:dvrv-us-1:devo/0:product/1'
  AND t.applies_to_part_id != 'don:core:dvrv-us-1:devo/0:product/1'
  AND COALESCE(f.name, cap.name) IS NOT NULL
GROUP BY COALESCE(f.name, cap.name)
ORDER BY ticket_count DESC
LIMIT 100`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query, warnings } = result as DecomposeResult;
        expect(tableSchema.name).toBe('t');
        expect(tableSchema.measures.map((m) => m.name)).toEqual([
          'avg_resolution_hours',
          'median_resolution_hours',
          'ticket_count',
        ]);
        const filters = flattenFilters(query.filters!);
        expect(filters).toContainEqual({
          member: 't.state',
          operator: 'equals',
          values: ['closed'],
        });
        expect(filters).not.toContainEqual(
          expect.objectContaining({ member: 'ap.product_id' })
        );
        expect(filters).toContainEqual({
          member: 't.applies_to_part_id',
          operator: 'notEquals',
          values: ['don:core:dvrv-us-1:devo/0:product/1'],
        });
        expect(warnings).toContainEqual(
          'Non-extractable WHERE condition retained in base SQL'
        );
        expect(query.order).toEqual({ 't.ticket_count': 'desc' });
        expect(query.limit).toBe(100);
      });
    });

    describe('Aggregates only (no GROUP BY dimensions)', () => {
      it('classifies all wrapped aggregates as measures', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT
  ROUND(AVG(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 1440.0, 1) AS avg_resolution_days,
  ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 1440.0, 1) AS median_resolution_days,
  ROUND(CAST(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 1440.0, 1) AS p25_resolution_days,
  ROUND(CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 1440.0, 1) AS p75_resolution_days,
  ROUND(MIN(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 1440.0, 1) AS min_resolution_days,
  ROUND(MAX(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 1440.0, 1) AS max_resolution_days,
  COUNT(*) AS total_closed_tickets
FROM devrev.ticket t
WHERE t.state = 'closed'
  AND t.actual_close_date IS NOT NULL
  AND t.created_date IS NOT NULL
  AND t.applies_to_part_id = 'don:core:dvrv-us-1:devo/0:capability/61'`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query } = result as DecomposeResult;
        expect(tableSchema.name).toBe('t');
        expect(tableSchema.sql).toBe('SELECT * FROM devrev.ticket AS t');
        expect(tableSchema.measures.map((m) => m.name)).toEqual([
          'avg_resolution_days',
          'median_resolution_days',
          'p25_resolution_days',
          'p75_resolution_days',
          'min_resolution_days',
          'max_resolution_days',
          'total_closed_tickets',
        ]);
        const filters = flattenFilters(query.filters!);
        expect(filters).toContainEqual({
          member: 't.applies_to_part_id',
          operator: 'equals',
          values: ['don:core:dvrv-us-1:devo/0:capability/61'],
        });
        expect(query.order).toBeUndefined();
        expect(query.limit).toBeUndefined();
      });
    });

    describe('DATE_TRUNC GROUP BY with TIMESTAMPTZ filter', () => {
      it('extracts all filters including TIMESTAMPTZ comparison', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT
  DATE_TRUNC('month', t.actual_close_date) AS month,
  ROUND(AVG(DATEDIFF('minute', t.created_date, t.actual_close_date)) / 1440.0, 1) AS avg_resolution_days,
  ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('minute', t.created_date, t.actual_close_date)) AS DOUBLE) / 1440.0, 1) AS median_resolution_days,
  COUNT(*) AS tickets_closed
FROM devrev.ticket t
WHERE t.state = 'closed'
  AND t.actual_close_date IS NOT NULL
  AND t.created_date IS NOT NULL
  AND t.applies_to_part_id = 'don:core:dvrv-us-1:devo/0:capability/61'
  AND t.actual_close_date >= TIMESTAMPTZ '2025-06-01T00:00:00+00:00'
GROUP BY DATE_TRUNC('month', t.actual_close_date)
ORDER BY month`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query, warnings } = result as DecomposeResult;
        expect(tableSchema.name).toBe('t');
        expect(tableSchema.measures.map((m) => m.name)).toEqual([
          'avg_resolution_days',
          'median_resolution_days',
          'tickets_closed',
        ]);
        expect(tableSchema.dimensions.map((d) => d.name)).toContain('month');
        const filters = flattenFilters(query.filters!);
        expect(filters).toContainEqual({
          member: 't.state',
          operator: 'equals',
          values: ['closed'],
        });
        expect(filters).toContainEqual({
          member: 't.applies_to_part_id',
          operator: 'equals',
          values: ['don:core:dvrv-us-1:devo/0:capability/61'],
        });
        expect(filters).toContainEqual({
          member: 't.actual_close_date',
          operator: 'gte',
          values: ['2025-06-01T00:00:00+00:00'],
        });
        expect(warnings).toEqual([]);
        expect(query.order).toEqual({ 't.month': 'asc' });
      });
    });

    describe('Subquery with window function + outer filter', () => {
      it('decomposes outer SELECT as non-aggregated with filter and multi-column order', async () => {
        const result = await sqlToMeerkat({
          sql: `SELECT part_name, severity, ticket_count
FROM (
  SELECT
    COALESCE(f.name, cap.name) AS part_name,
    t.severity,
    COUNT(*) AS ticket_count,
    SUM(COUNT(*)) OVER (PARTITION BY COALESCE(f.name, cap.name)) AS total_for_part
  FROM devrev.ticket t
  CROSS JOIN UNNEST(t.ancestral_product) AS ap(product_id)
  LEFT JOIN devrev.feature f ON t.applies_to_part_id = f.id
  LEFT JOIN devrev.capability cap ON t.applies_to_part_id = cap.id
  WHERE ap.product_id = 'don:core:dvrv-us-1:devo/0:product/1'
    AND t.applies_to_part_id != 'don:core:dvrv-us-1:devo/0:product/1'
    AND COALESCE(f.name, cap.name) IS NOT NULL
  GROUP BY COALESCE(f.name, cap.name), t.severity
) sub
WHERE total_for_part >= 40
ORDER BY total_for_part DESC, part_name, severity
LIMIT 100`,
          getQueryOutput,
        });
        expect(result.success).toBe(true);
        const { tableSchema, query, warnings } = result as DecomposeResult;
        expect(tableSchema.name).toBe('sub');
        expect(tableSchema.sql).toBe(
          `SELECT * FROM (SELECT COALESCE(f."name", cap."name") AS part_name, t.severity, count_star() AS ticket_count, sum(count_star()) OVER (PARTITION BY COALESCE(f."name", cap."name")) AS total_for_part FROM devrev.ticket AS t , unnest(t.ancestral_product) AS ap(product_id) LEFT JOIN devrev.feature AS f ON ((t.applies_to_part_id = f.id)) LEFT JOIN devrev.capability AS cap ON ((t.applies_to_part_id = cap.id)) WHERE ((ap.product_id = 'don:core:dvrv-us-1:devo/0:product/1') AND (t.applies_to_part_id != 'don:core:dvrv-us-1:devo/0:product/1') AND (COALESCE(f."name", cap."name") IS NOT NULL)) GROUP BY COALESCE(f."name", cap."name"), t.severity) AS sub`
        );
        expect(tableSchema.measures).toEqual([]);
        expect(tableSchema.dimensions.map((d) => d.name)).toEqual([
          'part_name',
          'severity',
          'ticket_count',
          'total_for_part',
        ]);
        const filters = flattenFilters(query.filters!);
        expect(filters).toContainEqual({
          member: 'sub.total_for_part',
          operator: 'gte',
          values: ['40'],
        });
        expect(query.order).toEqual({
          'sub.total_for_part': 'desc',
          'sub.part_name': 'asc',
          'sub.severity': 'asc',
        });
        expect(query.limit).toBe(100);
        expect(warnings).toEqual([]);
      });
    });
  });
});
