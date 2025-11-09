import { useMemo, useState } from 'react';
import { useDBM } from '../hooks/dbm-context';

interface BenchmarkResult {
  filterSize: number;
  queryType: string;
  time: number;
  success: boolean;
  error?: string;
}

interface AggregatedResult {
  queryType: string;
  results: { filterSize: number; time: number }[];
  p50: number;
  p75: number;
  p90: number;
}

export const InOperatorBenchmarking = () => {
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState('');
  const { dbm } = useDBM();

  const filterSizes = [0, 5, 10, 100, 1000, 100000];

  const generateFilterValues = (size: number): string[] => {
    const values: string[] = [];
    for (let i = 0; i < size; i++) {
      values.push(`don:core:dvrv-in-1:devo/2sRI6Hepzz:product/${i}`);
    }
    return values;
  };

  const createSyntheticDataQuery = () => {
    return `
      CREATE OR REPLACE TABLE dim_issue AS
      SELECT 
        'ISS-' || i AS id,
        CASE WHEN random() > 0.5 THEN 'open' ELSE 'in_progress' END AS state,
        ['USR-' || (random() * 100)::INTEGER] AS owned_by_ids,
        '{}' AS stage_json,
        CASE 
          WHEN random() > 0.66 THEN 'high'
          WHEN random() > 0.33 THEN 'medium'
          ELSE 'low'
        END AS priority,
        'pse' AS subtype,
        CURRENT_DATE - (random() * 365)::INTEGER AS created_date,
        '{"ctype__pse_pod": "pod_' || (random() * 10)::INTEGER || 
         '", "ctype__severity": "' || CASE 
            WHEN random() > 0.66 THEN 'sev1'
            WHEN random() > 0.33 THEN 'sev2'
            ELSE 'sev3'
          END || 
         '", "ctype__merchant_category": "cat_' || (random() * 5)::INTEGER || 
         '", "ctype__reported_team": "team_' || (random() * 8)::INTEGER || 
         '", "ctype__account_type": "type_' || (random() * 3)::INTEGER || 
         '", "ctype__cause_code": "code_' || (random() * 20)::INTEGER || 
         '", "ctype__escalated_to_dev": "' || (CASE WHEN random() > 0.7 THEN 'yes' ELSE 'no' END) || 
         '", "ctype__invalid_reason": "reason_' || (random() * 5)::INTEGER || 
         '", "ctype__issue_category": "category_' || (random() * 10)::INTEGER || '"}' AS custom_fields,
        'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/' || (random() * 100)::INTEGER AS ctype_pse__engineering_pod
      FROM generate_series(1, 10000) AS t(i);

      CREATE OR REPLACE TABLE dim_sla_tracker AS
      SELECT 
        'ISS-' || i AS applies_to_id,
        CASE 
          WHEN random() > 0.75 THEN 'breached'
          WHEN random() > 0.5 THEN 'at_risk'
          WHEN random() > 0.25 THEN 'on_track'
          ELSE 'not_started'
        END AS status
      FROM generate_series(1, 8000) AS t(i);
    `;
  };

  const generateQuery1_OriginalIN = (filterValues: string[]) => {
    const inClause =
      filterValues.length > 0
        ? `AND (dim_pse_ageing__engineering_pod IN (${filterValues
            .map((v) => `'${v}'`)
            .join(', ')}))`
        : '';

    return `
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025')) ${inClause}))
    `;
  };

  const generateQuery2_JoinWithTemp = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      CREATE TEMPORARY TABLE IF NOT EXISTS temp_filter_values AS
      SELECT unnest([${filterValues
        .map((v) => `'${v}'`)
        .join(', ')}]) AS filter_value;

      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      INNER JOIN temp_filter_values tfv ON dim_pse_ageing.engineering_pod = tfv.filter_value
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025'))));
      
      DROP TABLE temp_filter_values;
    `;
  };

  const generateQuery3_ANYOperator = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025')) 
        AND (dim_pse_ageing__engineering_pod = ANY([${filterValues
          .map((v) => `'${v}'`)
          .join(', ')}]))))
    `;
  };

  const generateQuery4_ExistsSubquery = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025')) 
        AND EXISTS (SELECT 1 FROM (VALUES ${filterValues
          .map((v) => `('${v}')`)
          .join(
            ', '
          )}) AS t(val) WHERE t.val = dim_pse_ageing__engineering_pod)))
    `;
  };

  const generateQuery5_ListContains = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025')) 
        AND list_contains([${filterValues
          .map((v) => `'${v}'`)
          .join(', ')}], dim_pse_ageing__engineering_pod)))
    `;
  };

  const generateQuery6_CTEWithFilter = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      WITH filter_values AS (
        SELECT unnest([${filterValues
          .map((v) => `'${v}'`)
          .join(', ')}]) AS pod_id
      ),
      base_data AS (
        SELECT 'issue' AS type,
               di.id,
               di.state,
               Unnest(di.owned_by_ids) AS owner,
               di.stage_json,
               di.priority,
               di.subtype,
               di.created_date,
               Monthname(di.created_date) AS month_name,
               Year(di.created_date) AS year,
               ctype_pse__engineering_pod AS engineering_pod,
               Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
               Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
               Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
               Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
               Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
               Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
               Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
               Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
               Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
               dst.status AS sla_status
        FROM dim_issue di
        LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
        WHERE di.subtype IN ('pse')
          AND (di.state = 'open' OR di.state = 'in_progress')
      ),
      filtered_data AS (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM base_data
        INNER JOIN filter_values ON base_data.engineering_pod = filter_values.pod_id
        WHERE type IN ('issue') 
          AND subtype IN ('pse') 
          AND Cast(year AS STRING) IN ('2025')
      )
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM filtered_data
    `;
  };

  const generateQuery7_CTEWithValues = (filterValues: string[]) => {
    if (filterValues.length === 0) {
      return generateQuery1_OriginalIN(filterValues);
    }

    return `
      WITH pods(pod) AS (
        VALUES
          ${filterValues.map((v) => `('${v}')`).join(',\n          ')}
      )
      SELECT Avg(Datediff('day', created_date, CURRENT_DATE)) AS dim_pse_ageing__avg_ageing
      FROM (
        SELECT type AS dim_pse_ageing__type,
               subtype AS dim_pse_ageing__subtype,
               Cast(year AS STRING) AS dim_pse_ageing__year,
               engineering_pod AS dim_pse_ageing__engineering_pod,
               *
        FROM (
          SELECT 'issue' AS type,
                 di.id,
                 di.state,
                 Unnest(di.owned_by_ids) AS owner,
                 di.stage_json,
                 di.priority,
                 di.subtype,
                 di.created_date,
                 Monthname(di.created_date) AS month_name,
                 Year(di.created_date) AS year,
                 ctype_pse__engineering_pod AS engineering_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__pse_pod') AS pse_pod,
                 Json_extract_string(di.custom_fields, '$.ctype__severity') AS severity,
                 Json_extract_string(di.custom_fields, '$.ctype__merchant_category') AS merchant_category,
                 Json_extract_string(di.custom_fields, '$.ctype__reported_team') AS reported_team,
                 Json_extract_string(di.custom_fields, '$.ctype__account_type') AS account_type,
                 Json_extract_string(di.custom_fields, '$.ctype__cause_code') AS cause_code,
                 Json_extract_string(di.custom_fields, '$.ctype__escalated_to_dev') AS escalated_to_dev,
                 Json_extract_string(di.custom_fields, '$.ctype__invalid_reason') AS invalid_reason,
                 Json_extract_string(di.custom_fields, '$.ctype__issue_category') AS issue_category,
                 dst.status AS sla_status
          FROM dim_issue di
          LEFT JOIN dim_sla_tracker dst ON di.id = dst.applies_to_id
          WHERE di.subtype IN ('pse')
            AND (di.state = 'open' OR di.state = 'in_progress')
        ) AS dim_pse_ageing
      ) AS dim_pse_ageing
      JOIN pods p ON p.pod = dim_pse_ageing__engineering_pod
      WHERE (((dim_pse_ageing__type IN ('issue')) AND (dim_pse_ageing__subtype IN ('pse')) AND (dim_pse_ageing__year IN ('2025'))))
    `;
  };

  const runBenchmark = async (
    queryType: string,
    queryGenerator: (vals: string[]) => string,
    filterSize: number
  ): Promise<BenchmarkResult> => {
    const filterValues = generateFilterValues(filterSize);
    const query = queryGenerator(filterValues);

    try {
      const start = performance.now();
      await dbm.query(query);
      const end = performance.now();

      return {
        filterSize,
        queryType,
        time: end - start,
        success: true,
      };
    } catch (error) {
      return {
        filterSize,
        queryType,
        time: 0,
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  };

  const validateQueryResults = async () => {
    setProgress('Validating that all queries return the same results...');

    const testSize = 10; // Use a small size for validation
    const filterValues = generateFilterValues(testSize);

    const queries = [
      { name: '1. Original IN', fn: generateQuery1_OriginalIN },
      { name: '2. JOIN with Temp Table', fn: generateQuery2_JoinWithTemp },
      { name: '3. ANY Operator', fn: generateQuery3_ANYOperator },
      { name: '4. EXISTS Subquery', fn: generateQuery4_ExistsSubquery },
      { name: '5. list_contains', fn: generateQuery5_ListContains },
      { name: '6. CTE with Filter', fn: generateQuery6_CTEWithFilter },
      { name: '7. CTE with VALUES', fn: generateQuery7_CTEWithValues },
    ];

    const results: { name: string; avgValue: number; rowCount: number }[] = [];

    for (const query of queries) {
      const originalQuery = query.fn(filterValues);

      // Get the aggregated value from the original query
      const valueQuery = originalQuery;

      // Also get the count of underlying rows before aggregation by wrapping the inner query
      // Extract the part before aggregation - remove the outer SELECT AVG and wrap with COUNT
      const innerQueryMatch = originalQuery.match(
        /FROM\s+\(([\s\S]+)\)\s+(?:AS|JOIN)/i
      );
      let rowCount = 0;

      if (innerQueryMatch) {
        const innerQuery = innerQueryMatch[1];
        const countQuery = `SELECT COUNT(*) as count FROM (${innerQuery}) as validation_inner`;
        try {
          const countResult = await dbm.query(countQuery);
          rowCount = countResult.toArray()[0]?.count || 0;
        } catch {
          // If count extraction fails, skip it
          rowCount = -1;
        }
      }

      try {
        const result = await dbm.query(valueQuery);
        const resultArray = result.toArray();
        const avgValue = resultArray[0]?.dim_pse_ageing__avg_ageing || 0;
        results.push({ name: query.name, avgValue, rowCount });
        setProgress(
          `Validated ${query.name}: avg=${avgValue.toFixed(
            2
          )}, rows=${rowCount}`
        );
      } catch (error) {
        setProgress(
          `Error validating ${query.name}: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
        throw error;
      }
    }

    // Check if all avg values are the same (within floating point tolerance)
    const firstAvg = results[0]?.avgValue;
    const tolerance = 0.01; // Allow small floating point differences

    const allAvgsSame = results.every(
      (r) => Math.abs(r.avgValue - firstAvg) < tolerance
    );

    if (!allAvgsSame) {
      const mismatchDetails = results
        .map((r) => `${r.name}: avg=${r.avgValue.toFixed(2)}`)
        .join('; ');
      throw new Error(`Query result mismatch! ${mismatchDetails}`);
    }

    // Find the most common row count (for informational purposes)
    const rowCounts = results
      .filter((r) => r.rowCount > 0)
      .map((r) => r.rowCount);
    const mostCommonRowCount = rowCounts.length > 0 ? rowCounts[0] : 'N/A';

    setProgress(
      `✓ All queries validated! All return avg=${firstAvg.toFixed(
        2
      )} (underlying rows: ~${mostCommonRowCount})`
    );
    await new Promise((resolve) => setTimeout(resolve, 2000)); // Show message for 2 seconds
  };

  const runAllBenchmarks = async () => {
    setIsRunning(true);
    setResults([]);
    setProgress('Initializing database and creating synthetic data...');

    try {
      // Create synthetic data
      await dbm.query(createSyntheticDataQuery());

      // Validate queries return same results
      await validateQueryResults();

      setProgress('Running benchmarks...');

      const allResults: BenchmarkResult[] = [];
      let completedTests = 0;
      const totalTests = filterSizes.length * 7; // 7 query types

      // Run benchmarks for each filter size and query type
      for (const size of filterSizes) {
        // Original IN operator
        setProgress(
          `Testing Original IN operator with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result1 = await runBenchmark(
          '1. Original IN',
          generateQuery1_OriginalIN,
          size
        );
        allResults.push(result1);
        setResults([...allResults]);

        // JOIN with temp table
        setProgress(
          `Testing JOIN with temp table with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result2 = await runBenchmark(
          '2. JOIN with Temp Table',
          generateQuery2_JoinWithTemp,
          size
        );
        allResults.push(result2);
        setResults([...allResults]);

        // ANY operator
        setProgress(
          `Testing ANY operator with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result3 = await runBenchmark(
          '3. ANY Operator',
          generateQuery3_ANYOperator,
          size
        );
        allResults.push(result3);
        setResults([...allResults]);

        // EXISTS subquery
        setProgress(
          `Testing EXISTS subquery with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result4 = await runBenchmark(
          '4. EXISTS Subquery',
          generateQuery4_ExistsSubquery,
          size
        );
        allResults.push(result4);
        setResults([...allResults]);

        // list_contains
        setProgress(
          `Testing list_contains with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result5 = await runBenchmark(
          '5. list_contains',
          generateQuery5_ListContains,
          size
        );
        allResults.push(result5);
        setResults([...allResults]);

        // CTE with filter
        setProgress(
          `Testing CTE with filter with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result6 = await runBenchmark(
          '6. CTE with Filter',
          generateQuery6_CTEWithFilter,
          size
        );
        allResults.push(result6);
        setResults([...allResults]);

        // CTE with VALUES
        setProgress(
          `Testing CTE with VALUES with ${size} filters... (${++completedTests}/${totalTests})`
        );
        const result7 = await runBenchmark(
          '7. CTE with VALUES',
          generateQuery7_CTEWithValues,
          size
        );
        allResults.push(result7);
        setResults([...allResults]);
      }

      setProgress('Benchmarks completed!');
    } catch (error) {
      setProgress(
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    } finally {
      setIsRunning(false);
    }
  };

  const calculatePercentile = (
    values: number[],
    percentile: number
  ): number => {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const index = (percentile / 100) * (sorted.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const weight = index - lower;
    return sorted[lower] * (1 - weight) + sorted[upper] * weight;
  };

  const aggregatedResults = useMemo(() => {
    const grouped = new Map<string, AggregatedResult>();

    results.forEach((result) => {
      if (!result.success) return;

      if (!grouped.has(result.queryType)) {
        grouped.set(result.queryType, {
          queryType: result.queryType,
          results: [],
          p50: 0,
          p75: 0,
          p90: 0,
        });
      }

      const group = grouped.get(result.queryType);
      if (group) {
        group.results.push({
          filterSize: result.filterSize,
          time: result.time,
        });
      }
    });

    // Calculate percentiles
    grouped.forEach((group) => {
      const times = group.results.map((r) => r.time);
      group.p50 = calculatePercentile(times, 50);
      group.p75 = calculatePercentile(times, 75);
      group.p90 = calculatePercentile(times, 90);
      group.results.sort((a, b) => a.filterSize - b.filterSize);
    });

    return Array.from(grouped.values()).sort((a, b) => a.p50 - b.p50);
  }, [results]);

  return (
    <div style={{ padding: '20px', fontFamily: 'monospace' }}>
      <h2>IN Operator Optimization Benchmarking</h2>
      <p>
        Testing different query strategies with varying filter sizes:{' '}
        {filterSizes.join(', ')}
      </p>

      <button
        onClick={runAllBenchmarks}
        disabled={isRunning}
        style={{
          padding: '10px 20px',
          fontSize: '16px',
          cursor: isRunning ? 'not-allowed' : 'pointer',
          backgroundColor: isRunning ? '#ccc' : '#007bff',
          color: 'white',
          border: 'none',
          borderRadius: '4px',
          marginBottom: '20px',
        }}
      >
        {isRunning ? 'Running...' : 'Run Benchmarks'}
      </button>

      {progress && (
        <div
          style={{
            padding: '10px',
            backgroundColor: '#f0f0f0',
            marginBottom: '20px',
            borderRadius: '4px',
          }}
        >
          {progress}
        </div>
      )}

      {aggregatedResults.length > 0 && (
        <div style={{ marginTop: '20px' }}>
          <h3>Results Summary (Ranked by p50 Median Time)</h3>
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              marginBottom: '30px',
            }}
          >
            <thead>
              <tr style={{ backgroundColor: '#f0f0f0' }}>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'left',
                  }}
                >
                  Rank
                </th>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'left',
                  }}
                >
                  Query Type
                </th>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'right',
                  }}
                >
                  p50 (ms)
                </th>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'right',
                  }}
                >
                  p75 (ms)
                </th>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'right',
                  }}
                >
                  p90 (ms)
                </th>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'right',
                  }}
                >
                  vs Best p50
                </th>
              </tr>
            </thead>
            <tbody>
              {aggregatedResults.map((result, index) => {
                const percentDiff =
                  index === 0
                    ? '0%'
                    : `+${Math.round(
                        ((result.p50 - aggregatedResults[0].p50) /
                          aggregatedResults[0].p50) *
                          100
                      )}%`;

                return (
                  <tr
                    key={result.queryType}
                    style={{
                      backgroundColor: index === 0 ? '#d4edda' : 'white',
                    }}
                  >
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {index === 0
                        ? '🥇'
                        : index === 1
                        ? '🥈'
                        : index === 2
                        ? '🥉'
                        : index + 1}
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {result.queryType}
                    </td>
                    <td
                      style={{
                        padding: '10px',
                        border: '1px solid #ddd',
                        textAlign: 'right',
                      }}
                    >
                      {result.p50.toFixed(2)}
                    </td>
                    <td
                      style={{
                        padding: '10px',
                        border: '1px solid #ddd',
                        textAlign: 'right',
                      }}
                    >
                      {result.p75.toFixed(2)}
                    </td>
                    <td
                      style={{
                        padding: '10px',
                        border: '1px solid #ddd',
                        textAlign: 'right',
                      }}
                    >
                      {result.p90.toFixed(2)}
                    </td>
                    <td
                      style={{
                        padding: '10px',
                        border: '1px solid #ddd',
                        textAlign: 'right',
                      }}
                    >
                      {percentDiff}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          <h3>Detailed Results by Filter Size</h3>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ backgroundColor: '#f0f0f0' }}>
                <th
                  style={{
                    padding: '10px',
                    border: '1px solid #ddd',
                    textAlign: 'left',
                  }}
                >
                  Query Type
                </th>
                {filterSizes.map((size) => (
                  <th
                    key={size}
                    style={{
                      padding: '10px',
                      border: '1px solid #ddd',
                      textAlign: 'right',
                    }}
                  >
                    {size} filters
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {aggregatedResults.map((result) => {
                // Find baseline (Original IN) results for comparison
                const baselineResult = aggregatedResults.find(
                  (r) => r.queryType === '1. Original IN'
                );

                return (
                  <tr key={result.queryType}>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {result.queryType}
                    </td>
                    {filterSizes.map((size) => {
                      const sizeResult = result.results.find(
                        (r) => r.filterSize === size
                      );
                      const baselineTime = baselineResult?.results.find(
                        (r) => r.filterSize === size
                      )?.time;

                      let displayText = 'N/A';
                      if (sizeResult) {
                        const timeStr = sizeResult.time.toFixed(2);
                        let percentChange = '';

                        if (
                          baselineTime &&
                          result.queryType !== '1. Original IN'
                        ) {
                          const diff =
                            ((sizeResult.time - baselineTime) / baselineTime) *
                            100;
                          const sign = diff > 0 ? '+' : '';
                          percentChange = ` (${sign}${diff.toFixed(0)}%)`;
                        }

                        displayText = `${timeStr}ms${percentChange}`;
                      }

                      return (
                        <td
                          key={size}
                          style={{
                            padding: '10px',
                            border: '1px solid #ddd',
                            textAlign: 'right',
                            backgroundColor:
                              result.queryType === '1. Original IN'
                                ? '#fff3cd'
                                : 'transparent',
                          }}
                        >
                          {displayText}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {results.some((r) => !r.success) && (
        <div style={{ marginTop: '20px' }}>
          <h3>Errors</h3>
          {results
            .filter((r) => !r.success)
            .map((result, index) => (
              <div
                key={index}
                style={{
                  padding: '10px',
                  backgroundColor: '#f8d7da',
                  marginBottom: '10px',
                  borderRadius: '4px',
                  color: '#721c24',
                }}
              >
                <strong>
                  {result.queryType} (Filter size: {result.filterSize}):
                </strong>{' '}
                {result.error}
              </div>
            ))}
        </div>
      )}

      <div
        style={{
          marginTop: '40px',
          padding: '20px',
          backgroundColor: '#f8f9fa',
          borderRadius: '4px',
        }}
      >
        <h3>Query Strategies Being Tested:</h3>
        <ol style={{ lineHeight: '1.8' }}>
          <li>
            <strong>Original IN:</strong> Standard SQL IN operator with list of
            values
          </li>
          <li>
            <strong>JOIN with Temp Table:</strong> Creates a temporary table and
            performs INNER JOIN
          </li>
          <li>
            <strong>ANY Operator:</strong> Uses = ANY([values]) syntax
          </li>
          <li>
            <strong>EXISTS Subquery:</strong> Uses EXISTS with VALUES subquery
          </li>
          <li>
            <strong>list_contains:</strong> Uses DuckDB's list_contains function
          </li>
          <li>
            <strong>CTE with Filter:</strong> Uses Common Table Expressions to
            structure the query with filter values CTE and INNER JOIN
          </li>
          <li>
            <strong>CTE with VALUES:</strong> Uses SQL standard VALUES clause in
            CTE with JOIN for filtering
          </li>
        </ol>
      </div>
    </div>
  );
};
