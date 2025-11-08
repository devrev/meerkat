/**
 * Real-world query optimization benchmark
 * Testing different optimization strategies on the actual production query pattern
 */

export const POD_VALUES_29 = [
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/6',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/10',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/33',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/180',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/3133',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14904',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12394',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:capability/17',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/16',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14829',
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
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11991',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11985',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11997',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11994',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12555',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11150',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11993',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11995',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11992',
];

interface QueryVariant {
  id: string;
  name: string;
  description: string;
  optimizations: string[];
  query: string;
}

// Create base query parts for reuse
const getBaseInnerQuery = (includeFilters: boolean) => {
  const baseQuery = `
    SELECT 
      'issue' AS type,
      id,
      state,
      subtype,
      created_date,
      YEAR(created_date) AS year,
      engineering_pod
    FROM test_data
  `;

  if (includeFilters) {
    return `${baseQuery}
    WHERE subtype IN ('pse')
      AND (state = 'open' OR state = 'in_progress')
      AND type = 'issue'
      AND CAST(year AS STRING) = '2025'
      AND engineering_pod IN (${POD_VALUES_29.map((v) => `'${v}'`).join(', ')})
    `;
  }
  
  return `${baseQuery}
    WHERE subtype IN ('pse')
      AND (state = 'open' OR state = 'in_progress')
  `;
};

const getBaseInnerQueryWithANY = (includeFilters: boolean) => {
  const baseQuery = `
    SELECT 
      'issue' AS type,
      id,
      state,
      subtype,
      created_date,
      YEAR(created_date) AS year,
      engineering_pod
    FROM test_data
  `;

  if (includeFilters) {
    return `${baseQuery}
    WHERE subtype = ANY(['pse']::VARCHAR[])
      AND (state = 'open' OR state = 'in_progress')
      AND type = ANY(['issue']::VARCHAR[])
      AND CAST(year AS STRING) = ANY(['2025']::VARCHAR[])
      AND engineering_pod = ANY([${POD_VALUES_29.map((v) => `'${v}'`).join(', ')}]::VARCHAR[])
    `;
  }
  
  return `${baseQuery}
    WHERE subtype = ANY(['pse']::VARCHAR[])
      AND (state = 'open' OR state = 'in_progress')
  `;
};

export const REAL_QUERY_VARIANTS: QueryVariant[] = [
  // 1. BASELINE - Original query pattern (filters at the end)
  {
    id: 'baseline',
    name: 'Baseline (Original)',
    description: 'Original query with late filters using IN operator',
    optimizations: [],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM (
          ${getBaseInnerQuery(false)}
        ) AS inner_query
      ) AS outer_query
      WHERE (
        dim_type IN ('issue')
        AND dim_subtype IN ('pse')
        AND dim_year IN ('2025')
        AND dim_engineering_pod IN (${POD_VALUES_29.map((v) => `'${v}'`).join(', ')})
      )
    `,
  },

  // 2. IN with Filter Pushdown
  {
    id: 'in_pushdown',
    name: 'IN + Filter Pushdown',
    description: 'IN operator with filters applied in innermost query',
    optimizations: ['Filter Pushdown'],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM (
          ${getBaseInnerQuery(true)}
        ) AS inner_query
      ) AS outer_query
    `,
  },

  // 3. ANY with Late Filters
  {
    id: 'any_late',
    name: 'ANY + Late Filters',
    description: 'ANY operator with filters at outer level',
    optimizations: ['ANY Operator'],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM (
          ${getBaseInnerQueryWithANY(false)}
        ) AS inner_query
      ) AS outer_query
      WHERE (
        dim_type = ANY(['issue']::VARCHAR[])
        AND dim_subtype = ANY(['pse']::VARCHAR[])
        AND dim_year = ANY(['2025']::VARCHAR[])
        AND dim_engineering_pod = ANY([${POD_VALUES_29.map((v) => `'${v}'`).join(', ')}]::VARCHAR[])
      )
    `,
  },

  // 4. ANY with Filter Pushdown
  {
    id: 'any_pushdown',
    name: 'ANY + Filter Pushdown',
    description: 'ANY operator with filters in innermost query',
    optimizations: ['ANY Operator', 'Filter Pushdown'],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM (
          ${getBaseInnerQueryWithANY(true)}
        ) AS inner_query
      ) AS outer_query
    `,
  },

  // 5. CTE with IN and Late Filters
  {
    id: 'cte_in_late',
    name: 'CTE + IN + Late Filters',
    description: 'Using CTE with IN operator and late filters',
    optimizations: ['CTE'],
    query: `
      WITH base_data AS (
        ${getBaseInnerQuery(false)}
      ),
      renamed_data AS (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM base_data
      )
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM renamed_data
      WHERE (
        dim_type IN ('issue')
        AND dim_subtype IN ('pse')
        AND dim_year IN ('2025')
        AND dim_engineering_pod IN (${POD_VALUES_29.map((v) => `'${v}'`).join(', ')})
      )
    `,
  },

  // 6. CTE with IN and Filter Pushdown
  {
    id: 'cte_in_pushdown',
    name: 'CTE + IN + Filter Pushdown',
    description: 'Using CTE with IN operator and early filters',
    optimizations: ['CTE', 'Filter Pushdown'],
    query: `
      WITH base_data AS (
        ${getBaseInnerQuery(true)}
      ),
      renamed_data AS (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM base_data
      )
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM renamed_data
    `,
  },

  // 7. CTE with ANY and Late Filters
  {
    id: 'cte_any_late',
    name: 'CTE + ANY + Late Filters',
    description: 'Using CTE with ANY operator and late filters',
    optimizations: ['CTE', 'ANY Operator'],
    query: `
      WITH base_data AS (
        ${getBaseInnerQueryWithANY(false)}
      ),
      renamed_data AS (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM base_data
      )
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM renamed_data
      WHERE (
        dim_type = ANY(['issue']::VARCHAR[])
        AND dim_subtype = ANY(['pse']::VARCHAR[])
        AND dim_year = ANY(['2025']::VARCHAR[])
        AND dim_engineering_pod = ANY([${POD_VALUES_29.map((v) => `'${v}'`).join(', ')}]::VARCHAR[])
      )
    `,
  },

  // 8. CTE with ANY and Filter Pushdown - BEST COMBINATION
  {
    id: 'cte_any_pushdown',
    name: 'CTE + ANY + Filter Pushdown',
    description: 'Using CTE with ANY operator and early filters (expected best)',
    optimizations: ['CTE', 'ANY Operator', 'Filter Pushdown'],
    query: `
      WITH base_data AS (
        ${getBaseInnerQueryWithANY(true)}
      ),
      renamed_data AS (
        SELECT 
          type AS dim_type,
          subtype AS dim_subtype,
          CAST(year AS STRING) AS dim_year,
          engineering_pod AS dim_engineering_pod,
          *
        FROM base_data
      )
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM renamed_data
    `,
  },

  // 9. Column Pruning + IN + Filter Pushdown
  {
    id: 'pruning_in_pushdown',
    name: 'Column Pruning + IN + Pushdown',
    description: 'Select only needed columns with IN and early filters',
    optimizations: ['Column Pruning', 'Filter Pushdown'],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT created_date
        FROM (
          ${getBaseInnerQuery(true)}
        ) AS inner_query
      ) AS outer_query
    `,
  },

  // 10. Column Pruning + ANY + Filter Pushdown
  {
    id: 'pruning_any_pushdown',
    name: 'Column Pruning + ANY + Pushdown',
    description: 'Select only needed columns with ANY and early filters',
    optimizations: ['Column Pruning', 'ANY Operator', 'Filter Pushdown'],
    query: `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT created_date
        FROM (
          ${getBaseInnerQueryWithANY(true)}
        ) AS inner_query
      ) AS outer_query
    `,
  },

  // 11. CTE + Column Pruning + ANY + Filter Pushdown - ULTIMATE OPTIMIZATION
  {
    id: 'ultimate',
    name: 'Ultimate (All Optimizations)',
    description: 'CTE + Column Pruning + ANY + Filter Pushdown',
    optimizations: ['CTE', 'Column Pruning', 'ANY Operator', 'Filter Pushdown'],
    query: `
      WITH base_data AS (
        ${getBaseInnerQueryWithANY(true)}
      )
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (SELECT created_date FROM base_data) AS pruned
    `,
  },
];

export interface QueryBenchmarkResult {
  variantId: string;
  variantName: string;
  optimizations: string[];
  executionTime: number; // Average time
  minTime: number;
  maxTime: number;
  stdDev: number;
  allRuns: number[];
  rank: number;
}

