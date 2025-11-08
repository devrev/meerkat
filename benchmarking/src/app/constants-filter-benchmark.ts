/**
 * Benchmark queries to compare IN vs ANY operator performance
 * Tests with different value set sizes: 5, 10, 20, 30 values
 * Uses synthetic data generated in-memory
 */

// Engineering pod values (matching production example with 30 values)
export const POD_VALUES_5 = [
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/6',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/10',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/33',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/180',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/3133',
];

export const POD_VALUES_10 = [
  ...POD_VALUES_5,
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14904',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12394',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:capability/17',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/16',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14829',
];

export const POD_VALUES_20 = [
  ...POD_VALUES_10,
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/13499',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/13396',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14723',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/181',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:capability/97',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12558',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/27',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14539',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12706',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/3795',
];

export const POD_VALUES_30 = [
  ...POD_VALUES_20,
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11991',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11985',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11997',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11994',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12555',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11150',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11993',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11995',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11992',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11996',
];

export const POD_VALUES_50 = [
  ...POD_VALUES_30,
  ...Array.from(
    { length: 20 },
    (_, i) => `don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/${20000 + i}`
  ),
];

export const POD_VALUES_100 = [
  ...POD_VALUES_50,
  ...Array.from(
    { length: 50 },
    (_, i) => `don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/${30000 + i}`
  ),
];

export const POD_VALUES_500 = [
  ...POD_VALUES_100,
  ...Array.from(
    { length: 400 },
    (_, i) => `don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/${40000 + i}`
  ),
];

export const POD_VALUES_1000 = [
  ...POD_VALUES_500,
  ...Array.from(
    { length: 500 },
    (_, i) => `don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/${50000 + i}`
  ),
];

export interface BenchmarkQuery {
  name: string;
  query: string;
  description: string;
}

/**
 * Create test queries comparing IN vs ANY operators
 */
export const createFilterBenchmarkQueries = (): BenchmarkQuery[] => {
  const queries: BenchmarkQuery[] = [];

  // Test 1: Small set (5 values) - IN operator
  queries.push({
    name: 'IN_5_values',
    description: 'IN operator with 5 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_5.map((v) => `'${v}'`).join(', ')})
    `,
  });

  // Test 2: Small set (5 values) - ANY operator
  queries.push({
    name: 'ANY_5_values',
    description: 'ANY operator with 5 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_5.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 3: Medium set (10 values) - IN operator
  queries.push({
    name: 'IN_10_values',
    description: 'IN operator with 10 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_10.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 4: Medium set (10 values) - ANY operator
  queries.push({
    name: 'ANY_10_values',
    description: 'ANY operator with 10 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_10.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 5: Large set (20 values) - IN operator
  queries.push({
    name: 'IN_20_values',
    description: 'IN operator with 20 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_20.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 6: Large set (20 values) - ANY operator
  queries.push({
    name: 'ANY_20_values',
    description: 'ANY operator with 20 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_20.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 7: Very large set (30 values) - IN operator (matches your production example)
  queries.push({
    name: 'IN_30_values',
    description: 'IN operator with 30 values (production scenario)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 8: Very large set (30 values) - ANY operator
  queries.push({
    name: 'ANY_30_values',
    description: 'ANY operator with 30 values (production scenario)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 9: Complex query with aggregation - IN operator (20 values)
  queries.push({
    name: 'IN_20_with_aggregation',
    description: 'IN operator with 20 values in complex query with aggregation',
    query: `
      SELECT 
        type,
        subtype,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_20.map((v) => `'${v}'`).join(
        ', '
      )})
      GROUP BY type, subtype
      ORDER BY count DESC
    `,
  });

  // Test 10: Complex query with aggregation - ANY operator (20 values)
  queries.push({
    name: 'ANY_20_with_aggregation',
    description:
      'ANY operator with 20 values in complex query with aggregation',
    query: `
      SELECT 
        type,
        subtype,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_20.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
      GROUP BY type, subtype
      ORDER BY count DESC
    `,
  });

  // Test 11: Multiple filters with IN (30 values) - Production-like query
  queries.push({
    name: 'IN_30_multiple_filters',
    description:
      'IN operator with 30 values and additional filters (production-like)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )})
        AND type = 'issue'
        AND subtype = 'pse'
        AND year = '2025'
        AND state IN ('open', 'in_progress')
    `,
  });

  // Test 12: Multiple filters with ANY (30 values) - Production-like query
  queries.push({
    name: 'ANY_30_multiple_filters',
    description:
      'ANY operator with 30 values and additional filters (production-like)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_30.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
        AND type = 'issue'
        AND subtype = 'pse'
        AND year = '2025'
        AND state IN ('open', 'in_progress')
    `,
  });

  // Test 13: Large set (50 values) - IN operator
  queries.push({
    name: 'IN_50_values',
    description: 'IN operator with 50 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_50.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 14: Large set (50 values) - ANY operator
  queries.push({
    name: 'ANY_50_values',
    description: 'ANY operator with 50 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_50.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 15: Very large set (100 values) - IN operator
  queries.push({
    name: 'IN_100_values',
    description: 'IN operator with 100 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_100.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 16: Very large set (100 values) - ANY operator
  queries.push({
    name: 'ANY_100_values',
    description: 'ANY operator with 100 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_100.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 17: Extreme set (500 values) - IN operator
  queries.push({
    name: 'IN_500_values',
    description: 'IN operator with 500 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_500.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 18: Extreme set (500 values) - ANY operator
  queries.push({
    name: 'ANY_500_values',
    description: 'ANY operator with 500 values',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_500.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 19: Maximum set (1000 values) - IN operator
  queries.push({
    name: 'IN_1000_values',
    description: 'IN operator with 1000 values (stress test)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_1000.map((v) => `'${v}'`).join(
        ', '
      )})
    `,
  });

  // Test 20: Maximum set (1000 values) - ANY operator
  queries.push({
    name: 'ANY_1000_values',
    description: 'ANY operator with 1000 values (stress test)',
    query: `
      SELECT COUNT(*) as count
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_1000.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
    `,
  });

  // Test 21: Aggregation with 50 values - IN operator
  queries.push({
    name: 'IN_50_with_aggregation',
    description: 'IN operator with 50 values and aggregation',
    query: `
      SELECT 
        state,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles,
        MAX(trip_duration) as max_duration
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_50.map((v) => `'${v}'`).join(
        ', '
      )})
      GROUP BY state
    `,
  });

  // Test 22: Aggregation with 50 values - ANY operator
  queries.push({
    name: 'ANY_50_with_aggregation',
    description: 'ANY operator with 50 values and aggregation',
    query: `
      SELECT 
        state,
        COUNT(*) as count,
        AVG(trip_miles) as avg_miles,
        MAX(trip_duration) as max_duration
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_50.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
      GROUP BY state
    `,
  });

  // Test 23: Complex query with 100 values - IN operator
  queries.push({
    name: 'IN_100_complex',
    description: 'IN operator with 100 values, joins and multiple conditions',
    query: `
      SELECT 
        year,
        type,
        COUNT(*) as total,
        AVG(trip_miles) as avg_miles
      FROM test_data
      WHERE engineering_pod IN (${POD_VALUES_100.map((v) => `'${v}'`).join(
        ', '
      )})
        AND state IN ('open', 'in_progress')
        AND trip_miles > 1.0
      GROUP BY year, type
      ORDER BY total DESC
    `,
  });

  // Test 24: Complex query with 100 values - ANY operator
  queries.push({
    name: 'ANY_100_complex',
    description: 'ANY operator with 100 values, joins and multiple conditions',
    query: `
      SELECT 
        year,
        type,
        COUNT(*) as total,
        AVG(trip_miles) as avg_miles
      FROM test_data
      WHERE engineering_pod = ANY([${POD_VALUES_100.map((v) => `'${v}'`).join(
        ', '
      )}]::VARCHAR[])
        AND state IN ('open', 'in_progress')
        AND trip_miles > 1.0
      GROUP BY year, type
      ORDER BY total DESC
    `,
  });

  // Test 25: Subquery with 30 values - IN operator
  queries.push({
    name: 'IN_30_subquery',
    description: 'IN operator with 30 values in subquery',
    query: `
      SELECT type, subtype, total_count
      FROM (
        SELECT 
          type,
          subtype,
          COUNT(*) as total_count
        FROM test_data
        WHERE engineering_pod IN (${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )})
        GROUP BY type, subtype
      ) AS sub
      WHERE total_count > 10
      ORDER BY total_count DESC
    `,
  });

  // Test 26: Subquery with 30 values - ANY operator
  queries.push({
    name: 'ANY_30_subquery',
    description: 'ANY operator with 30 values in subquery',
    query: `
      SELECT type, subtype, total_count
      FROM (
        SELECT 
          type,
          subtype,
          COUNT(*) as total_count
        FROM test_data
        WHERE engineering_pod = ANY([${POD_VALUES_30.map((v) => `'${v}'`).join(
          ', '
        )}]::VARCHAR[])
        GROUP BY type, subtype
      ) AS sub
      WHERE total_count > 10
      ORDER BY total_count DESC
    `,
  });

  return queries;
};

// Export the queries as a constant array for use in benchmarking
export const FILTER_BENCHMARK_QUERIES = createFilterBenchmarkQueries();

// Helper function to get query pairs for comparison
export const getQueryPairs = (): Array<{
  in: BenchmarkQuery;
  any: BenchmarkQuery;
  category: string;
}> => {
  const queries = FILTER_BENCHMARK_QUERIES;
  return [
    { in: queries[0], any: queries[1], category: 'Simple: 5 values' },
    { in: queries[2], any: queries[3], category: 'Simple: 10 values' },
    { in: queries[4], any: queries[5], category: 'Simple: 20 values' },
    { in: queries[6], any: queries[7], category: 'Simple: 30 values' },
    { in: queries[8], any: queries[9], category: 'Aggregation: 20 values' },
    { in: queries[10], any: queries[11], category: 'Multi-filter: 30 values' },
    { in: queries[12], any: queries[13], category: 'Simple: 50 values' },
    { in: queries[14], any: queries[15], category: 'Simple: 100 values' },
    { in: queries[16], any: queries[17], category: 'Simple: 500 values' },
    { in: queries[18], any: queries[19], category: 'Simple: 1000 values' },
    { in: queries[20], any: queries[21], category: 'Aggregation: 50 values' },
    { in: queries[22], any: queries[23], category: 'Complex: 100 values' },
    { in: queries[24], any: queries[25], category: 'Subquery: 30 values' },
  ];
};
