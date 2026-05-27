import { DecomposeResult, sqlToMeerkat } from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const getQueryOutput = (query: string) =>
  duckdbExec<Record<string, string>[]>(query);

describe('sqlToMeerkat — production queries', () => {
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
      expect(tableSchema.dimensions.map((d) => d.name)).toContain('part_name');
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
      const { query } = result as DecomposeResult;
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
      expect(filters).toContainEqual({
        member: 't.product_id',
        operator: 'equals',
        values: ['don:core:dvrv-us-1:devo/0:product/1'],
      });
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
    it('extracts extractable filters, retains TIMESTAMPTZ comparison in residual', async () => {
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
      expect(warnings).toContainEqual(
        'Non-extractable WHERE condition retained in base SQL'
      );
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function flattenFilters(filters: any[]): any[] {
  const result: any[] = [];
  for (const f of filters) {
    if ('and' in f) {
      result.push(...flattenFilters(f.and));
    } else if ('or' in f) {
      result.push(...flattenFilters(f.or));
    } else {
      result.push(f);
    }
  }
  return result;
}
