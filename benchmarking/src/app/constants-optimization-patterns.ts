/**
 * Additional query optimization patterns beyond IN vs ANY
 * These test alternative approaches to common performance bottlenecks
 */

import { POD_VALUES_30 } from './constants-filter-benchmark';

export interface OptimizationPattern {
  name: string;
  description: string;
  query: string;
  category: string;
}

/**
 * Pattern 1: Filter Pushdown
 * Apply filters as early as possible in the query
 */
const filterPushdownPattern: OptimizationPattern[] = [
  // Simple COUNT with 30 values
  {
    name: 'LATE_FILTER_30',
    description: 'Late filter: 30 values (SLOW)',
    category: 'Filter Pushdown (30 values)',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, year
        FROM test_data
      ) AS filtered
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
        AND type = 'issue'
        AND year = '2025'
    `,
  },
  {
    name: 'EARLY_FILTER_30',
    description: 'Early filter: 30 values (FAST)',
    category: 'Filter Pushdown (30 values)',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, year
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
          AND type = 'issue'
          AND year = '2025'
      ) AS filtered
    `,
  },

  // With aggregation
  {
    name: 'LATE_FILTER_AGG',
    description: 'Late filter with aggregation (SLOW)',
    category: 'Filter Pushdown (with aggregation)',
    query: `
      SELECT 
        year,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles
      FROM (
        SELECT engineering_pod, type, year, trip_miles
        FROM test_data
      ) AS data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
        AND type = 'issue'
      GROUP BY year
    `,
  },
  {
    name: 'EARLY_FILTER_AGG',
    description: 'Early filter with aggregation (FAST)',
    category: 'Filter Pushdown (with aggregation)',
    query: `
      SELECT 
        year,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles
      FROM (
        SELECT engineering_pod, type, year, trip_miles
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
          AND type = 'issue'
      ) AS data
      GROUP BY year
    `,
  },

  // Multiple filter conditions
  {
    name: 'LATE_FILTER_MULTI',
    description: 'Late filter with multiple conditions (SLOW)',
    category: 'Filter Pushdown (multiple filters)',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, subtype, year, state
        FROM test_data
      ) AS data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
        AND type = 'issue'
        AND subtype = 'pse'
        AND year = '2025'
        AND state IN ('open', 'in_progress')
    `,
  },
  {
    name: 'EARLY_FILTER_MULTI',
    description: 'Early filter with multiple conditions (FAST)',
    category: 'Filter Pushdown (multiple filters)',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, subtype, year, state
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
          AND type = 'issue'
          AND subtype = 'pse'
          AND year = '2025'
          AND state IN ('open', 'in_progress')
      ) AS data
    `,
  },
];

/**
 * Pattern 2: Column Pruning
 * Only select columns you actually need
 */
const columnPruningPattern: OptimizationPattern[] = [
  {
    name: 'SELECT_STAR',
    description: 'SELECT * pulls all columns (SLOW)',
    category: 'Column Pruning',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT *
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
      ) AS data
      WHERE type = 'issue'
    `,
  },
  {
    name: 'SELECT_SPECIFIC',
    description: 'SELECT only needed columns (FAST)',
    category: 'Column Pruning',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
      ) AS data
      WHERE type = 'issue'
    `,
  },
];

/**
 * Pattern 3: Redundant Filter Elimination
 * Don't filter on the same condition multiple times
 */
const redundantFilterPattern: OptimizationPattern[] = [
  {
    name: 'REDUNDANT_FILTERS',
    description: 'Same filter applied at multiple levels (SLOW)',
    category: 'Filter Deduplication',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, subtype
        FROM test_data
        WHERE type = 'issue' AND subtype = 'pse'
      ) AS inner_data
      WHERE type = 'issue' AND subtype = 'pse'
        AND engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
    `,
  },
  {
    name: 'NO_REDUNDANT_FILTERS',
    description: 'Each filter applied once (FAST)',
    category: 'Filter Deduplication',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, subtype
        FROM test_data
        WHERE type = 'issue' 
          AND subtype = 'pse'
          AND engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
            ', '
          )})
      ) AS inner_data
    `,
  },
];

/**
 * Pattern 4: Subquery vs JOIN
 * Sometimes JOIN is faster than IN with subquery
 */
const subqueryVsJoinPattern: OptimizationPattern[] = [
  {
    name: 'IN_SUBQUERY',
    description: 'Using IN with subquery (can be slow)',
    category: 'Subquery vs JOIN',
    query: `
      SELECT COUNT(*) as count
      FROM test_data t1
      WHERE t1.engineering_pod IN (
        SELECT engineering_pod 
        FROM test_data t2
        WHERE t2.type = 'issue' 
        LIMIT 30
      )
    `,
  },
  {
    name: 'SEMI_JOIN',
    description: 'Using SEMI JOIN (often faster)',
    category: 'Subquery vs JOIN',
    query: `
      SELECT COUNT(*) as count
      FROM test_data t1
      WHERE EXISTS (
        SELECT 1 
        FROM test_data t2
        WHERE t2.engineering_pod = t1.engineering_pod
          AND t2.type = 'issue'
        LIMIT 1
      )
      LIMIT 30
    `,
  },
];

/**
 * Pattern 5: Aggregation Before vs After Filter
 * Pre-aggregate when possible
 */
const aggregationOrderPattern: OptimizationPattern[] = [
  {
    name: 'FILTER_THEN_AGGREGATE',
    description: 'Filter full dataset then aggregate (baseline)',
    category: 'Aggregation Order',
    query: `
      SELECT 
        year,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
        AND type = 'issue'
      GROUP BY year
    `,
  },
  {
    name: 'AGGREGATE_WITH_FILTER',
    description: 'Combine filter and aggregate (often same)',
    category: 'Aggregation Order',
    query: `
      SELECT 
        year,
        COUNT(CASE WHEN type = 'issue' THEN 1 END) as count,
        AVG(CASE WHEN type = 'issue' THEN trip_miles END) as avg_miles
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
      GROUP BY year
    `,
  },
];

/**
 * Pattern 6: CTE vs Subquery
 * CTEs can sometimes be optimized better
 */
const cteVsSubqueryPattern: OptimizationPattern[] = [
  {
    name: 'NESTED_SUBQUERY',
    description: 'Nested subqueries (can be hard to optimize)',
    category: 'CTE vs Subquery',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod, type, year
        FROM (
          SELECT *
          FROM test_data
          WHERE type = 'issue'
        ) AS level1
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
      ) AS level2
      WHERE year = '2025'
    `,
  },
  {
    name: 'WITH_CTE',
    description: 'Common Table Expression (more readable, similar perf)',
    category: 'CTE vs Subquery',
    query: `
      WITH level1 AS (
        SELECT *
        FROM test_data
        WHERE type = 'issue'
      ),
      level2 AS (
        SELECT engineering_pod, type, year
        FROM level1
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
      )
      SELECT COUNT(*) as count
      FROM level2
      WHERE year = '2025'
    `,
  },
];

/**
 * Pattern 7: DISTINCT vs GROUP BY
 * For getting unique values
 */
const distinctVsGroupByPattern: OptimizationPattern[] = [
  {
    name: 'DISTINCT',
    description: 'Using DISTINCT',
    category: 'DISTINCT vs GROUP BY',
    query: `
      SELECT COUNT(DISTINCT engineering_pod) as count
      FROM test_data
      WHERE type = 'issue'
        AND year = '2025'
    `,
  },
  {
    name: 'GROUP_BY',
    description: 'Using GROUP BY (sometimes faster)',
    category: 'DISTINCT vs GROUP BY',
    query: `
      SELECT COUNT(*) as count
      FROM (
        SELECT engineering_pod
        FROM test_data
        WHERE type = 'issue'
          AND year = '2025'
        GROUP BY engineering_pod
      ) AS grouped
    `,
  },
];

/**
 * Pattern 8: Multiple OR conditions vs IN
 * OR conditions can prevent index usage
 */
const orVsInPattern: OptimizationPattern[] = [
  {
    name: 'MULTIPLE_OR',
    description: 'Multiple OR conditions (SLOW)',
    category: 'OR vs IN',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE type = 'issue' OR type = 'ticket' OR type = 'task'
    `,
  },
  {
    name: 'USE_IN',
    description: 'Using IN clause (FAST)',
    category: 'OR vs IN',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE type IN ('issue', 'ticket', 'task')
    `,
  },
];

// Export all patterns - currently only using filter pushdown
export const OPTIMIZATION_PATTERNS: OptimizationPattern[] = [
  ...filterPushdownPattern,
  // Uncomment below to test other patterns:
  ...columnPruningPattern,
  ...redundantFilterPattern,
  ...subqueryVsJoinPattern,
  ...aggregationOrderPattern,
  ...cteVsSubqueryPattern,
  ...distinctVsGroupByPattern,
  ...orVsInPattern,
];

// Helper to get pattern pairs for comparison
export const getOptimizationPairs = (): Array<{
  slow: OptimizationPattern;
  fast: OptimizationPattern;
  category: string;
}> => {
  const pairs: Array<{
    slow: OptimizationPattern;
    fast: OptimizationPattern;
    category: string;
  }> = [];

  for (let i = 0; i < OPTIMIZATION_PATTERNS.length; i += 2) {
    if (i + 1 < OPTIMIZATION_PATTERNS.length) {
      pairs.push({
        slow: OPTIMIZATION_PATTERNS[i],
        fast: OPTIMIZATION_PATTERNS[i + 1],
        category: OPTIMIZATION_PATTERNS[i].category,
      });
    }
  }

  return pairs;
};
