/**
 * Scalability benchmark - Testing how optimizations perform with different filter value counts
 */

// Generate pod values for different scales
export const generatePodValues = (count: number): string[] => {
  const pods: string[] = [];
  for (let i = 0; i < count; i++) {
    pods.push(`don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/${1000 + i}`);
  }
  return pods;
};

export const POD_VALUES_1 = generatePodValues(1);
export const POD_VALUES_2 = generatePodValues(2);
export const POD_VALUES_3 = generatePodValues(3);
export const POD_VALUES_5 = generatePodValues(5);
export const POD_VALUES_10 = generatePodValues(10);
export const POD_VALUES_20 = generatePodValues(20);
export const POD_VALUES_50 = generatePodValues(50);
export const POD_VALUES_100 = generatePodValues(100);
export const POD_VALUES_500 = generatePodValues(500);
export const POD_VALUES_1000 = generatePodValues(1000);

export interface ScalabilityVariant {
  id: string;
  name: string;
  optimizationType: string;
  valueCount: number;
  description: string;
  optimizations: string[];
  query: string;
}

// Helper to get base query with filters
const getBaseQuery = (
  podValues: string[],
  useANY: boolean,
  pushFilters: boolean,
  useCTE: boolean,
  pruneColumns: boolean
) => {
  const podList = useANY
    ? `[${podValues.map((v) => `'${v}'`).join(', ')}]::VARCHAR[]`
    : podValues.map((v) => `'${v}'`).join(', ');

  const filterCondition = useANY
    ? `engineering_pod = ANY(${podList})`
    : `engineering_pod IN (${podList})`;

  const baseFilters = `
    subtype ${useANY ? "= ANY(['pse']::VARCHAR[])" : "IN ('pse')"}
    AND (state = 'open' OR state = 'in_progress')
    AND type ${useANY ? "= ANY(['issue']::VARCHAR[])" : "IN ('issue')"}
    AND CAST(year AS STRING) ${
      useANY ? "= ANY(['2025']::VARCHAR[])" : "IN ('2025')"
    }
    AND ${filterCondition}
  `;

  const innerQuery = `
    SELECT 
      'issue' AS type,
      id,
      state,
      subtype,
      created_date,
      YEAR(created_date) AS year,
      engineering_pod
    FROM test_data
    ${
      pushFilters
        ? `WHERE ${baseFilters}`
        : "WHERE subtype IN ('pse') AND (state = 'open' OR state = 'in_progress')"
    }
  `;

  if (useCTE) {
    if (pruneColumns) {
      return `
        WITH base_data AS (${innerQuery})
        SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
        FROM (SELECT created_date FROM base_data) AS pruned
      `;
    }
    return `
      WITH base_data AS (${innerQuery}),
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
      ${pushFilters ? '' : `WHERE ${baseFilters}`}
    `;
  }

  if (pruneColumns) {
    return `
      SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
      FROM (
        SELECT created_date
        FROM (${innerQuery}) AS inner_query
      ) AS outer_query
    `;
  }

  return `
    SELECT AVG(DATEDIFF('day', created_date, CURRENT_DATE)) AS avg_ageing
    FROM (
      SELECT 
        type AS dim_type,
        subtype AS dim_subtype,
        CAST(year AS STRING) AS dim_year,
        engineering_pod AS dim_engineering_pod,
        *
      FROM (${innerQuery}) AS inner_query
    ) AS outer_query
    ${pushFilters ? '' : `WHERE ${baseFilters}`}
  `;
};

// ============================================================================
// COMPARATIVE TEST SCENARIOS: IN vs ANY under different conditions
// ============================================================================

interface ComparativeScenario {
  category: string;
  scenarios: {
    name: string;
    description: string;
    getQuery: (values: string[], useANY: boolean) => string;
  }[];
}

const COMPARATIVE_SCENARIOS: ComparativeScenario[] = [
  {
    category: 'Simple Queries (No Subqueries)',
    scenarios: [
      {
        name: 'Direct Filter Only',
        description: 'Single filter with no subqueries or aggregations',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `SELECT COUNT(*) as count FROM test_data WHERE ${condition}`;
        },
      },
      {
        name: 'Two Filters',
        description: 'Two independent filter conditions',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `SELECT COUNT(*) as count FROM test_data WHERE ${condition} AND state = 'open'`;
        },
      },
      {
        name: 'Simple Aggregation',
        description: 'Single filter with COUNT aggregation',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `SELECT engineering_pod, COUNT(*) as count FROM test_data WHERE ${condition} GROUP BY engineering_pod`;
        },
      },
    ],
  },
  {
    category: 'Subquery Scenarios',
    scenarios: [
      {
        name: 'Single Subquery Layer',
        description: 'One level of subquery nesting',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `SELECT COUNT(*) FROM (SELECT * FROM test_data WHERE state = 'open') AS sub WHERE ${condition}`;
        },
      },
      {
        name: 'Double Subquery Layer',
        description: 'Two levels of subquery nesting (like your real query)',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `
            SELECT AVG(trip_miles) FROM (
              SELECT * FROM (
                SELECT * FROM test_data WHERE state = 'open'
              ) AS inner_sub
            ) AS outer_sub WHERE ${condition}
          `;
        },
      },
      {
        name: 'Subquery with Multiple Filters',
        description: 'Nested subquery with multiple filter conditions',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const podCondition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          const typeCondition = useANY
            ? `type = ANY(['issue']::VARCHAR[])`
            : `type IN ('issue')`;
          return `
            SELECT COUNT(*) FROM (
              SELECT * FROM test_data WHERE state = 'open'
            ) AS sub WHERE ${podCondition} AND ${typeCondition}
          `;
        },
      },
    ],
  },
  {
    category: 'Complex Aggregations',
    scenarios: [
      {
        name: 'Multiple Aggregations',
        description: 'Multiple aggregate functions in single query',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `
            SELECT 
              COUNT(*) as count,
              AVG(trip_miles) as avg_miles,
              SUM(trip_duration) as total_duration
            FROM test_data WHERE ${condition}
          `;
        },
      },
      {
        name: 'Aggregation with GROUP BY',
        description: 'Aggregation with grouping and filtering',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `
            SELECT 
              type,
              COUNT(*) as count,
              AVG(trip_miles) as avg_miles
            FROM test_data 
            WHERE ${condition}
            GROUP BY type
            HAVING COUNT(*) > 10
          `;
        },
      },
    ],
  },
  {
    category: 'CTE Scenarios',
    scenarios: [
      {
        name: 'Simple CTE',
        description: 'Single CTE with filter',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `
            WITH filtered_data AS (
              SELECT * FROM test_data WHERE ${condition}
            )
            SELECT COUNT(*) as count FROM filtered_data
          `;
        },
      },
      {
        name: 'Multiple CTEs',
        description: 'Multiple CTEs with joins',
        getQuery: (values, useANY) => {
          const filterValues = values.map((v) => `'${v}'`).join(', ');
          const condition = useANY
            ? `engineering_pod = ANY([${filterValues}]::VARCHAR[])`
            : `engineering_pod IN (${filterValues})`;
          return `
            WITH base_data AS (
              SELECT * FROM test_data WHERE ${condition}
            ),
            aggregated AS (
              SELECT engineering_pod, COUNT(*) as count
              FROM base_data
              GROUP BY engineering_pod
            )
            SELECT AVG(count) as avg_count FROM aggregated
          `;
        },
      },
    ],
  },
  // Removed Join Scenarios - too expensive for quick testing
  // {
  //   category: 'Join Scenarios',
  //   scenarios: [
  //     {
  //       name: 'Self Join with Filter',
  //       description: 'Self-join with IN/ANY filter',
  //       getQuery: (values, useANY) => {
  //         const filterValues = values.map((v) => `'${v}'`).join(', ');
  //         const condition = useANY
  //           ? `t1.engineering_pod = ANY([${filterValues}]::VARCHAR[])`
  //           : `t1.engineering_pod IN (${filterValues})`;
  //         return `
  //           SELECT COUNT(*) FROM test_data t1
  //           INNER JOIN test_data t2 ON t1.type = t2.type
  //           WHERE ${condition}
  //         `;
  //       },
  //     },
  //   ],
  // },
];

// Generate variants for each optimization type at different scales
export const SCALABILITY_VARIANTS: ScalabilityVariant[] = [];

const optimizationTypes = [
  {
    type: 'Baseline (IN + Late Filter)',
    useANY: false,
    pushFilters: false,
    useCTE: false,
    pruneColumns: false,
    optimizations: [] as string[],
  },
  {
    type: 'IN + Filter Pushdown',
    useANY: false,
    pushFilters: true,
    useCTE: false,
    pruneColumns: false,
    optimizations: ['Filter Pushdown'],
  },
  {
    type: 'ANY + Late Filter',
    useANY: true,
    pushFilters: false,
    useCTE: false,
    pruneColumns: false,
    optimizations: ['ANY Operator'],
  },
  {
    type: 'ANY + Filter Pushdown',
    useANY: true,
    pushFilters: true,
    useCTE: false,
    pruneColumns: false,
    optimizations: ['ANY Operator', 'Filter Pushdown'],
  },
  {
    type: 'CTE + IN + Pushdown',
    useANY: false,
    pushFilters: true,
    useCTE: true,
    pruneColumns: false,
    optimizations: ['CTE', 'Filter Pushdown'],
  },
  {
    type: 'CTE + ANY + Pushdown',
    useANY: true,
    pushFilters: true,
    useCTE: true,
    pruneColumns: false,
    optimizations: ['CTE', 'ANY Operator', 'Filter Pushdown'],
  },
  {
    type: 'Column Pruning + IN + Pushdown',
    useANY: false,
    pushFilters: true,
    useCTE: false,
    pruneColumns: true,
    optimizations: ['Column Pruning', 'Filter Pushdown'],
  },
  {
    type: 'Column Pruning + ANY + Pushdown',
    useANY: true,
    pushFilters: true,
    useCTE: false,
    pruneColumns: true,
    optimizations: ['Column Pruning', 'ANY Operator', 'Filter Pushdown'],
  },
  {
    type: 'Ultimate (CTE + Pruning + ANY + Pushdown)',
    useANY: true,
    pushFilters: true,
    useCTE: true,
    pruneColumns: true,
    optimizations: ['CTE', 'Column Pruning', 'ANY Operator', 'Filter Pushdown'],
  },
];

const valueCounts = [
  { count: 5, values: POD_VALUES_5 },
  { count: 10, values: POD_VALUES_10 },
  { count: 100, values: POD_VALUES_100 },
  { count: 1000, values: POD_VALUES_1000 },
];

optimizationTypes.forEach((opt) => {
  valueCounts.forEach(({ count, values }) => {
    SCALABILITY_VARIANTS.push({
      id: `${opt.type.toLowerCase().replace(/\s+/g, '_')}_${count}`,
      name: `${opt.type} (${count} values)`,
      optimizationType: opt.type,
      valueCount: count,
      description: `${opt.type} with ${count} filter values`,
      optimizations: opt.optimizations,
      query: getBaseQuery(
        values,
        opt.useANY,
        opt.pushFilters,
        opt.useCTE,
        opt.pruneColumns
      ),
    });
  });
});

// ============================================================================
// GENERATE COMPARATIVE VARIANTS: Testing IN vs ANY across all scenarios
// ============================================================================

export const COMPARATIVE_VARIANTS: ScalabilityVariant[] = [];

// Test value counts to explore crossover points
// Reduced set for faster testing: 1, 5, 20, 100, 1000 (80 variants instead of 200)
const comparativeValueCounts = [
  { count: 1, values: POD_VALUES_1 },
  { count: 5, values: POD_VALUES_5 },
  { count: 20, values: POD_VALUES_20 },
  { count: 100, values: POD_VALUES_100 },
  { count: 1000, values: POD_VALUES_1000 },
];

// Generate IN vs ANY variants for each scenario
COMPARATIVE_SCENARIOS.forEach((category) => {
  category.scenarios.forEach((scenario) => {
    comparativeValueCounts.forEach(({ count, values }) => {
      // IN variant
      COMPARATIVE_VARIANTS.push({
        id: `in_${category.category
          .toLowerCase()
          .replace(/\s+/g, '_')}_${scenario.name
          .toLowerCase()
          .replace(/\s+/g, '_')}_${count}`,
        name: `IN: ${scenario.name} (${count} values)`,
        optimizationType: `${category.category} - ${scenario.name}`,
        valueCount: count,
        description: `${scenario.description} using IN operator with ${count} values`,
        optimizations: ['IN Operator'],
        query: scenario.getQuery(values, false),
      });

      // ANY variant
      COMPARATIVE_VARIANTS.push({
        id: `any_${category.category
          .toLowerCase()
          .replace(/\s+/g, '_')}_${scenario.name
          .toLowerCase()
          .replace(/\s+/g, '_')}_${count}`,
        name: `ANY: ${scenario.name} (${count} values)`,
        optimizationType: `${category.category} - ${scenario.name}`,
        valueCount: count,
        description: `${scenario.description} using ANY operator with ${count} values`,
        optimizations: ['ANY Operator'],
        query: scenario.getQuery(values, true),
      });
    });
  });
});

export interface ScalabilityBenchmarkResult {
  variantId: string;
  variantName: string;
  optimizationType: string;
  valueCount: number;
  optimizations: string[];
  executionTime: number;
  minTime: number;
  maxTime: number;
  stdDev: number;
  allRuns: number[];
  rank: number;
}

// Utility function to get comparative results
export const getComparativeAnalysis = (
  results: ScalabilityBenchmarkResult[]
) => {
  const analysis: {
    scenario: string;
    valueCount: number;
    inTime: number;
    anyTime: number;
    winner: 'IN' | 'ANY' | 'TIE';
    advantage: number; // percentage
  }[] = [];

  COMPARATIVE_SCENARIOS.forEach((category) => {
    category.scenarios.forEach((scenario) => {
      comparativeValueCounts.forEach(({ count }) => {
        const scenarioType = `${category.category} - ${scenario.name}`;
        const inResult = results.find(
          (r) =>
            r.optimizationType === scenarioType &&
            r.valueCount === count &&
            r.optimizations.includes('IN Operator')
        );
        const anyResult = results.find(
          (r) =>
            r.optimizationType === scenarioType &&
            r.valueCount === count &&
            r.optimizations.includes('ANY Operator')
        );

        if (inResult && anyResult) {
          const diff = Math.abs(
            inResult.executionTime - anyResult.executionTime
          );
          const faster =
            inResult.executionTime < anyResult.executionTime ? 'IN' : 'ANY';
          const slower =
            faster === 'IN' ? anyResult.executionTime : inResult.executionTime;
          const advantage = (diff / slower) * 100;

          analysis.push({
            scenario: scenarioType,
            valueCount: count,
            inTime: inResult.executionTime,
            anyTime: anyResult.executionTime,
            winner: advantage < 5 ? 'TIE' : faster, // Less than 5% difference is a tie
            advantage,
          });
        }
      });
    });
  });

  return analysis;
};
