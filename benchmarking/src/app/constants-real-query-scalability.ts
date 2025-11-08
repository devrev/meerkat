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

export const POD_VALUES_5 = generatePodValues(5);
export const POD_VALUES_10 = generatePodValues(10);
export const POD_VALUES_100 = generatePodValues(100);
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
    AND CAST(year AS STRING) ${useANY ? "= ANY(['2025']::VARCHAR[])" : "IN ('2025')"}
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
    ${pushFilters ? `WHERE ${baseFilters}` : "WHERE subtype IN ('pse') AND (state = 'open' OR state = 'in_progress')"}
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

