import React, { useEffect, useState } from 'react';
import {
  COMPARATIVE_VARIANTS,
  SCALABILITY_VARIANTS,
  ScalabilityBenchmarkResult,
  getComparativeAnalysis,
} from '../constants-real-query-scalability';
import { generateTestData } from '../generate-test-data';
import { useDBM } from '../hooks/dbm-context';

const ITERATIONS_PER_QUERY = 10; // Run each query 10 times for reliable median

interface RealQueryBenchmarkProps {
  comparative?: boolean;
}

export const RealQueryBenchmark = ({
  comparative = false,
}: RealQueryBenchmarkProps) => {
  const { dbm, fileManagerType } = useDBM();
  const VARIANTS = comparative ? COMPARATIVE_VARIANTS : SCALABILITY_VARIANTS;
  const [results, setResults] = useState<ScalabilityBenchmarkResult[]>([]);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [isRunning, setIsRunning] = useState(false);
  const [loadingData, setLoadingData] = useState(false);
  const [dataProgress, setDataProgress] = useState('');
  const [currentQuery, setCurrentQuery] = useState(0);
  const [currentIteration, setCurrentIteration] = useState(0);
  const [totalTime, setTotalTime] = useState(0);

  const isNodeEnvironment = fileManagerType === 'native';
  const environmentLabel = isNodeEnvironment
    ? 'Node.js (Native)'
    : 'Browser (WASM)';

  const toggleRow = (optimizationType: string) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(optimizationType)) {
      newExpanded.delete(optimizationType);
    } else {
      newExpanded.add(optimizationType);
    }
    setExpandedRows(newExpanded);
  };

  useEffect(() => {
    loadTestData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadTestData = async (): Promise<void> => {
    setLoadingData(true);
    setDataProgress('Generating 100,000 rows of synthetic data...');
    const testData = generateTestData(100000);
    console.log(`Generated ${testData.length} rows`);

    setDataProgress('Loading data into DuckDB...');
    const batchSize = 5000;
    for (let i = 0; i < testData.length; i += batchSize) {
      const batch = testData.slice(i, Math.min(i + batchSize, testData.length));
      const values = batch
        .map(
          (row) =>
            `('${row.id}', '${row.engineering_pod}', '${row.type}', '${row.subtype}', 
            '${row.year}', '${row.state}', '${row.priority}', '${row.created_date}', 
            ${row.trip_miles}, ${row.trip_duration}, '${row.base_num}', '${row.license_num}')`
        )
        .join(',');

      if (i === 0) {
        await dbm.query(`
          CREATE OR REPLACE TABLE test_data AS 
          SELECT 
            id::VARCHAR AS id,
            engineering_pod::VARCHAR AS engineering_pod,
            type::VARCHAR AS type,
            subtype::VARCHAR AS subtype,
            year::VARCHAR AS year,
            state::VARCHAR AS state,
            priority::VARCHAR AS priority,
            created_date::DATE AS created_date,
            trip_miles::DOUBLE AS trip_miles,
            trip_duration::DOUBLE AS trip_duration,
            base_num::VARCHAR AS base_num,
            license_num::VARCHAR AS license_num
          FROM (VALUES ${values}) AS t(id, engineering_pod, type, subtype, year, 
            state, priority, created_date, trip_miles, trip_duration, base_num, license_num)
        `);
      } else {
        await dbm.query(`INSERT INTO test_data VALUES ${values}`);
      }
      const progress = Math.round(((i + batch.length) / testData.length) * 100);
      setDataProgress(
        `Loading data: ${progress}% (${i + batch.length}/${
          testData.length
        } rows)`
      );
    }
    console.log('Test data loaded successfully');
    setLoadingData(false);
    setDataProgress('');
  };

  const calculateMedian = (values: number[]): number => {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
  };

  const formatTime = (ms: number): string => {
    // performance.now() returns milliseconds, verify units are correct
    if (ms < 0.001) {
      return `${(ms * 1000000).toFixed(2)} ns`;
    } else if (ms < 1) {
      return `${(ms * 1000).toFixed(2)} μs`;
    } else if (ms < 1000) {
      return `${ms.toFixed(2)} ms`;
    } else {
      return `${(ms / 1000).toFixed(2)} s`;
    }
  };

  const generateSlackMessage = (
    benchmarkResults: ScalabilityBenchmarkResult[],
    environment: string
  ): string => {
    // Debug: log the structure
    console.log('DEBUG: Sample result structure:', benchmarkResults[0]);
    console.log('DEBUG: Total results:', benchmarkResults.length);

    // Group results by optimization type
    const groupMap = new Map<string, ScalabilityBenchmarkResult[]>();
    benchmarkResults.forEach((result) => {
      const key = result.optimizationType;
      if (!groupMap.has(key)) {
        groupMap.set(key, []);
      }
      const group = groupMap.get(key);
      if (group) {
        group.push(result);
      }
    });

    const grouped = Array.from(groupMap.entries()).map(
      ([optimizationType, items]) => {
        const medianOfMedians =
          items.reduce((sum, item) => sum + item.executionTime, 0) /
          items.length;
        return {
          optimizationType,
          items,
          avgOfAverages: medianOfMedians, // Keep name for compatibility but it's now median
        };
      }
    );

    // Sort all groups by performance
    const sortedGroups = [...grouped].sort(
      (a, b) => a.avgOfAverages - b.avgOfAverages
    );

    const BASELINE_NAME = 'Baseline (IN + Late Filter)';
    const ANY_LATE_FILTER = 'ANY + Late Filter';
    const ANY_PUSHDOWN = 'ANY + Filter Pushdown';

    const baselineGroup = grouped.find(
      (g) => g.optimizationType === BASELINE_NAME
    );
    const baselineMedian = baselineGroup?.avgOfAverages || 0;

    let message = `*🚀 SQL Query Optimization Benchmark Results*\n\n`;
    message += `*Environment:* ${environment}\n`;
    message += `*Iterations per query:* ${ITERATIONS_PER_QUERY}\n`;
    message += `*Metric:* Median execution time (cold cache)\n`;
    message += `*Cache clearing:* CHECKPOINT before each run\n`;
    message += `*Test scales:* 5, 10, 100, 1000 filter values\n\n`;

    // Create main comparison table
    message += `*📊 Performance Comparison Table*\n`;
    message += `\`\`\`\n`;
    message += `╔═════╦══════════════════════════════════════╦════════════╦═══════════╦══════════╦══════════╦══════════╦══════════╗\n`;
    message += `║ Rnk ║ Optimization Strategy                ║ Med Time   ║ Improve % ║ 5 vals   ║ 10 vals  ║ 100 vals ║ 1000 vls ║\n`;
    message += `╠═════╬══════════════════════════════════════╬════════════╬═══════════╬══════════╬══════════╬══════════╬══════════╣\n`;

    sortedGroups.forEach((group, idx) => {
      const rank = idx + 1;
      const medal =
        rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : ` ${rank}`;

      const strategyName =
        group.optimizationType.length > 36
          ? group.optimizationType.substring(0, 33) + '...'
          : group.optimizationType.padEnd(36);

      const medianTimeStr = formatTime(group.avgOfAverages).padStart(10);

      let improvementStr = '';
      if (group.optimizationType === BASELINE_NAME) {
        improvementStr = '(baseline)'.padStart(9);
      } else {
        const improvement =
          baselineMedian > 0
            ? ((baselineMedian - group.avgOfAverages) / baselineMedian) * 100
            : 0;
        const sign = improvement > 0 ? '+' : '';
        improvementStr = `${sign}${improvement.toFixed(1)}%`.padStart(9);
      }

      // Get values for each scale
      const val5 = group.items.find((item) => item.valueCount === 5);
      const val10 = group.items.find((item) => item.valueCount === 10);
      const val100 = group.items.find((item) => item.valueCount === 100);
      const val1000 = group.items.find((item) => item.valueCount === 1000);

      const val5Str = val5
        ? formatTime(val5.executionTime).padStart(8)
        : 'N/A'.padStart(8);
      const val10Str = val10
        ? formatTime(val10.executionTime).padStart(8)
        : 'N/A'.padStart(8);
      const val100Str = val100
        ? formatTime(val100.executionTime).padStart(8)
        : 'N/A'.padStart(8);
      const val1000Str = val1000
        ? formatTime(val1000.executionTime).padStart(8)
        : 'N/A'.padStart(8);

      message += `║ ${medal} ║ ${strategyName} ║ ${medianTimeStr} ║ ${improvementStr} ║ ${val5Str} ║ ${val10Str} ║ ${val100Str} ║ ${val1000Str} ║\n`;
    });

    message += `╚═════╩══════════════════════════════════════╩════════════╩═══════════╩══════════╩══════════╩══════════╩══════════╝\n`;
    message += `\`\`\`\n\n`;

    // Add focused ANY comparison
    message += `*✨ ANY Operator Recommendation*\n`;
    const anyLateFilterGroup = sortedGroups.find(
      (g) => g.optimizationType === ANY_LATE_FILTER
    );
    const anyPushdownGroup = sortedGroups.find(
      (g) => g.optimizationType === ANY_PUSHDOWN
    );

    if (anyLateFilterGroup && baselineMedian > 0) {
      const anyLateImprovement =
        ((baselineMedian - anyLateFilterGroup.avgOfAverages) / baselineMedian) *
        100;
      const anyPushdownImprovement = anyPushdownGroup
        ? ((baselineMedian - anyPushdownGroup.avgOfAverages) / baselineMedian) *
          100
        : 0;

      message += `\`\`\`\n`;
      message += `Simple ANY (Late Filter):  ${formatTime(
        anyLateFilterGroup.avgOfAverages
      ).padStart(10)} median  (+${anyLateImprovement.toFixed(1)}%)\n`;
      if (anyPushdownGroup) {
        message += `ANY + Filter Pushdown:     ${formatTime(
          anyPushdownGroup.avgOfAverages
        ).padStart(10)} median  (+${anyPushdownImprovement.toFixed(1)}%)\n`;
      }
      message += `\`\`\`\n\n`;
    }

    message += `*💡 Recommendation*\n`;
    const bestNonBaseline = sortedGroups.find(
      (g) => g.optimizationType !== BASELINE_NAME
    );

    if (anyLateFilterGroup && baselineMedian > 0) {
      const anyLateImprovement =
        ((baselineMedian - anyLateFilterGroup.avgOfAverages) / baselineMedian) *
        100;
      const topImprovement = bestNonBaseline
        ? ((baselineMedian - bestNonBaseline.avgOfAverages) / baselineMedian) *
          100
        : 0;

      message += `> Switching from \`IN\` to \`ANY\` operator provides a *${anyLateImprovement.toFixed(
        1
      )}% performance improvement* with minimal code changes (just the filter syntax). This is the easiest optimization to implement and yields comparable results to more complex approaches. The best overall strategy (*${
        bestNonBaseline?.optimizationType || 'N/A'
      }*) achieves ${topImprovement.toFixed(
        1
      )}% improvement but requires additional refactoring.\n`;
    } else {
      message += `> Analysis in progress...\n`;
    }

    return message;
  };

  const runBenchmark = async () => {
    if (!dbm) return;

    setIsRunning(true);
    setResults([]);
    setCurrentQuery(0);
    const benchmarkResults: ScalabilityBenchmarkResult[] = [];
    const startTime = performance.now();

    console.log('\n🚀 Starting Real-World Query Optimization Benchmark\n');
    console.log('Environment:', environmentLabel);
    console.log('Testing', VARIANTS.length, 'query variants...');
    console.log(
      'Running each query',
      ITERATIONS_PER_QUERY,
      'times for reliable median values (with cache clearing)\n'
    );

    for (let i = 0; i < VARIANTS.length; i++) {
      const variant = VARIANTS[i];
      setCurrentQuery(i + 1);

      console.log(`\n📊 Testing Variant ${i + 1}/${VARIANTS.length}`);
      console.log(`Name: ${variant.name}`);
      console.log(
        `Optimizations: ${variant.optimizations.join(', ') || 'None'}`
      );

      const runTimes: number[] = [];

      // Run query multiple times
      for (let iteration = 0; iteration < ITERATIONS_PER_QUERY; iteration++) {
        setCurrentIteration(iteration + 1);

        // Clear cache before each run for fair benchmarking
        try {
          await dbm.query('CHECKPOINT');
          // Small delay to ensure cache is cleared
          await new Promise((resolve) => setTimeout(resolve, 10));
        } catch (error) {
          console.warn('⚠️ Could not clear cache:', error);
        }

        const iterationStart = performance.now();

        try {
          await dbm.query(variant.query);
        } catch (error) {
          console.error(`❌ Error executing variant ${variant.id}:`, error);
        }

        const iterationEnd = performance.now();
        const iterationTime = iterationEnd - iterationStart;
        runTimes.push(iterationTime);

        console.log(
          `  Run ${
            iteration + 1
          }/${ITERATIONS_PER_QUERY}: ${iterationTime.toFixed(2)}ms`
        );
      }

      // Calculate statistics
      const medianTime = calculateMedian(runTimes);
      const minTime = Math.min(...runTimes);
      const maxTime = Math.max(...runTimes);

      // Calculate standard deviation (using median as center)
      const squaredDiffs = runTimes.map((time) =>
        Math.pow(time - medianTime, 2)
      );
      const variance =
        squaredDiffs.reduce((a, b) => a + b, 0) / runTimes.length;
      const stdDev = Math.sqrt(variance);

      benchmarkResults.push({
        variantId: variant.id,
        variantName: variant.name,
        optimizationType: variant.optimizationType,
        valueCount: variant.valueCount,
        optimizations: variant.optimizations,
        executionTime: medianTime,
        minTime,
        maxTime,
        stdDev,
        allRuns: runTimes,
        rank: 0, // Will be calculated after all results are in
      });

      console.log(
        `✅ Median: ${medianTime.toFixed(2)}ms (min: ${minTime.toFixed(
          2
        )}ms, max: ${maxTime.toFixed(2)}ms, σ: ${stdDev.toFixed(2)}ms)`
      );
    }

    // Calculate rankings
    benchmarkResults.sort((a, b) => a.executionTime - b.executionTime);
    benchmarkResults.forEach((result, index) => {
      result.rank = index + 1;
    });

    // Sort by optimization type, then by value count
    benchmarkResults.sort((a, b) => {
      if (a.optimizationType !== b.optimizationType) {
        const aIndex = VARIANTS.findIndex(
          (v) => v.optimizationType === a.optimizationType
        );
        const bIndex = VARIANTS.findIndex(
          (v) => v.optimizationType === b.optimizationType
        );
        return aIndex - bIndex;
      }
      return a.valueCount - b.valueCount;
    });

    const endTime = performance.now();
    const totalBenchmarkTime = endTime - startTime;

    setResults(benchmarkResults);
    setTotalTime(totalBenchmarkTime);
    setIsRunning(false);

    // Console summary
    console.log('\n\n═══════════════════════════════════════════════════════');
    console.log('📊 BENCHMARK RESULTS SUMMARY');
    console.log('Environment:', environmentLabel);
    console.log('═══════════════════════════════════════════════════════\n');

    const sortedByPerformance = [...benchmarkResults].sort(
      (a, b) => a.executionTime - b.executionTime
    );

    console.table(
      sortedByPerformance.map((r) => ({
        Rank: r.rank,
        'Query Variant': r.variantName,
        'Execution Time (ms)': r.executionTime.toFixed(2),
        Optimizations: r.optimizations.join(', ') || 'None',
        'vs Baseline': `${(
          ((benchmarkResults[0].executionTime - r.executionTime) /
            benchmarkResults[0].executionTime) *
          100
        ).toFixed(1)}%`,
      }))
    );

    const best = sortedByPerformance[0];
    const baseline = benchmarkResults[0];

    console.log('\n🏆 WINNER:', best.variantName);
    console.log('⏱️  Best Time:', best.executionTime.toFixed(2), 'ms');
    console.log('🎯 Optimizations:', best.optimizations.join(', '));
    console.log(
      '📈 Improvement over baseline:',
      (
        ((baseline.executionTime - best.executionTime) /
          baseline.executionTime) *
        100
      ).toFixed(1),
      '%'
    );
    console.log(
      '⚡ Speedup Factor:',
      (baseline.executionTime / best.executionTime).toFixed(2),
      'x'
    );
    console.log('\n═══════════════════════════════════════════════════════\n');

    // Generate Slack-formatted output
    console.log('\n\n📋 SLACK MESSAGE FORMAT (Copy-paste ready):');
    console.log(
      '═════════════════════════════════════════════════════════════\n'
    );

    const slackMessage = generateSlackMessage(
      benchmarkResults,
      environmentLabel
    );
    console.log(slackMessage);

    console.log(
      '\n═════════════════════════════════════════════════════════════\n'
    );
  };

  const getBestResult = () => {
    if (results.length === 0) return null;
    return results.reduce((best, current) =>
      current.executionTime < best.executionTime ? current : best
    );
  };

  // Group results by optimization type
  const getGroupedResults = () => {
    const grouped = new Map<string, ScalabilityBenchmarkResult[]>();
    results.forEach((result) => {
      if (!grouped.has(result.optimizationType)) {
        grouped.set(result.optimizationType, []);
      }
      const group = grouped.get(result.optimizationType);
      if (group) {
        group.push(result);
      }
    });

    // Create groups with rankings
    const groups = Array.from(grouped.entries()).map(([type, variants]) => ({
      optimizationType: type,
      variants: variants.sort((a, b) => a.valueCount - b.valueCount),
      avgOfAverages:
        variants.reduce((sum, v) => sum + v.executionTime, 0) / variants.length,
      rank: 0, // Will be assigned below
    }));

    // Rank groups by average performance
    const sortedGroups = [...groups].sort(
      (a, b) => a.avgOfAverages - b.avgOfAverages
    );
    sortedGroups.forEach((group, index) => {
      const originalGroup = groups.find(
        (g) => g.optimizationType === group.optimizationType
      );
      if (originalGroup) {
        originalGroup.rank = index + 1;
      }
    });

    return groups;
  };

  // Get ranks within each scale (value count)
  const getRanksWithinScale = (valueCount: number) => {
    const variantsAtScale = results.filter((r) => r.valueCount === valueCount);
    const sorted = [...variantsAtScale].sort(
      (a, b) => a.executionTime - b.executionTime
    );
    const ranks = new Map<string, number>();
    sorted.forEach((variant, index) => {
      ranks.set(variant.variantId, index + 1);
    });
    return ranks;
  };

  // Get baseline for comparison (first optimization type's results)
  const getBaselineByScale = () => {
    const baselineType = getGroupedResults()[0]?.optimizationType;
    if (!baselineType) return new Map<number, number>();

    const baselineResults = results.filter(
      (r) => r.optimizationType === baselineType
    );
    const baselineMap = new Map<number, number>();
    baselineResults.forEach((r) => {
      baselineMap.set(r.valueCount, r.executionTime);
    });
    return baselineMap;
  };

  const getRankBadge = (rank: number, size: 'small' | 'large' = 'large') => {
    const fontSize = size === 'small' ? '0.9em' : '1.2em';
    if (rank === 1)
      return (
        <span style={{ fontSize }} role="img" aria-label="1st place gold medal">
          🥇
        </span>
      );
    if (rank === 2)
      return (
        <span
          style={{ fontSize }}
          role="img"
          aria-label="2nd place silver medal"
        >
          🥈
        </span>
      );
    if (rank === 3)
      return (
        <span
          style={{ fontSize }}
          role="img"
          aria-label="3rd place bronze medal"
        >
          🥉
        </span>
      );
    return (
      <span
        style={{ fontSize: size === 'small' ? '0.8em' : '1em', color: '#666' }}
      >
        #{rank}
      </span>
    );
  };

  const getImprovementBadge = (
    improvement: number,
    size: 'small' | 'large' = 'large'
  ) => {
    if (improvement === 0) return null;

    const fontSize = size === 'small' ? '0.75em' : '0.85em';
    const color = improvement > 0 ? '#2e7d32' : '#d32f2f';
    const icon = improvement > 0 ? '↑' : '↓';

    return (
      <span
        style={{
          fontSize,
          color,
          fontWeight: 'bold',
          marginLeft: '8px',
        }}
      >
        {icon} {Math.abs(improvement).toFixed(1)}%
      </span>
    );
  };

  const getOptimizationBadges = (optimizations: string[]) => {
    if (optimizations.length === 0) {
      return (
        <span
          style={{
            padding: '4px 8px',
            borderRadius: '4px',
            background: '#e0e0e0',
            fontSize: '0.85em',
            color: '#666',
          }}
        >
          None
        </span>
      );
    }

    return (
      <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
        {optimizations.map((opt, idx) => (
          <span
            key={idx}
            style={{
              padding: '4px 8px',
              borderRadius: '4px',
              background: '#2196f3',
              color: 'white',
              fontSize: '0.85em',
              fontWeight: '500',
            }}
          >
            {opt}
          </span>
        ))}
      </div>
    );
  };

  const bestResult = getBestResult();

  return (
    <div style={{ padding: '20px', maxWidth: '1400px', margin: '0 auto' }}>
      <h1 style={{ color: '#2c3e50', marginBottom: '10px' }}>
        <span role="img" aria-label="rocket">
          🚀
        </span>{' '}
        Real-World Query Optimization Benchmark
      </h1>
      <div
        style={{
          display: 'inline-block',
          padding: '6px 12px',
          background: isNodeEnvironment ? '#4caf50' : '#2196f3',
          color: 'white',
          borderRadius: '4px',
          fontSize: '0.9em',
          fontWeight: 'bold',
          marginBottom: '10px',
        }}
      >
        <span role="img" aria-label="environment">
          {isNodeEnvironment ? '⚙️' : '🌐'}
        </span>{' '}
        Environment: {environmentLabel}
      </div>
      <p style={{ color: '#666', marginBottom: '20px', fontSize: '1.1em' }}>
        {comparative
          ? `Testing ${VARIANTS.length} query variants: IN vs ANY comparison across 9 query scenarios at 5 key value counts (1, 5, 20, 100, 1000) - Estimated time: 3-5 minutes`
          : `Testing ${VARIANTS.length} query variants across 9 optimization strategies at 4 different scales (5, 10, 100, 1000 values)`}
      </p>

      <div
        style={{
          padding: '15px',
          background: '#e3f2fd',
          borderRadius: '8px',
          marginBottom: '20px',
          border: '2px solid #2196f3',
        }}
      >
        <p
          style={{
            margin: '0 0 10px 0',
            fontWeight: 'bold',
            fontSize: '1.1em',
          }}
        >
          <span role="img" aria-label="bar chart">
            📊
          </span>{' '}
          Benchmark Configuration
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Query Pattern:</strong> Nested subqueries with aggregation
          (AVG)
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Filter Values:</strong> 29 engineering pod IDs
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Optimizations Tested:</strong> IN vs ANY, Filter Pushdown,
          CTE, Column Pruning, and Combinations
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Dataset:</strong> 100,000 synthetic rows
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Iterations:</strong> Each query runs {ITERATIONS_PER_QUERY}{' '}
          times for reliable median measurements (cold cache)
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Cache Clearing:</strong> CHECKPOINT executed before each run
        </p>
        <p
          style={{
            margin: '10px 0 0 0',
            padding: '8px',
            background: '#fff9c4',
            borderRadius: '4px',
            fontSize: '0.95em',
          }}
        >
          <span role="img" aria-label="info">
            ℹ️
          </span>{' '}
          <strong>Note:</strong> The first query variant "Baseline (Original)"
          is your current query. All other variants are compared against it.
          <br />
          • ✓ Positive % (green) = faster than baseline = BETTER
          <br />
          • ✗ Negative % (red) = slower than baseline = WORSE
          <br />• Highlighted rows (green/yellow/orange) = Top 3 queries that
          beat the baseline
        </p>
      </div>

      {loadingData && (
        <div
          style={{
            padding: '20px',
            background: '#fff3cd',
            borderRadius: '8px',
            marginBottom: '20px',
          }}
        >
          <h3>
            <span role="img" aria-label="hourglass">
              ⏳
            </span>{' '}
            Loading Test Data
          </h3>
          <p>{dataProgress}</p>
        </div>
      )}

      <button
        onClick={runBenchmark}
        disabled={isRunning || loadingData}
        style={{
          padding: '15px 30px',
          fontSize: '1.1em',
          background: isRunning || loadingData ? '#ccc' : '#3498db',
          color: 'white',
          border: 'none',
          borderRadius: '8px',
          cursor: isRunning || loadingData ? 'not-allowed' : 'pointer',
          marginBottom: '20px',
          fontWeight: 'bold',
        }}
      >
        {isRunning
          ? `Running... (${currentQuery}/${VARIANTS.length})`
          : loadingData
          ? 'Loading Data...'
          : 'Run Benchmark'}
      </button>

      {isRunning && (
        <div
          style={{
            padding: '20px',
            background: '#f0f0f0',
            borderRadius: '8px',
            marginBottom: '20px',
          }}
        >
          <h3>
            <span role="img" aria-label="refresh">
              🔄
            </span>{' '}
            Running Benchmark
          </h3>
          <p>
            <strong>Query:</strong> {currentQuery} / {VARIANTS.length}
          </p>
          <p>
            <strong>Iteration:</strong> {currentIteration} /{' '}
            {ITERATIONS_PER_QUERY}
          </p>
          <p style={{ fontSize: '0.9em', color: '#666' }}>
            Running each query {ITERATIONS_PER_QUERY} times for accurate median
            values (clearing cache between runs)...
          </p>
        </div>
      )}

      {!isRunning && results.length > 0 && (
        <>
          <div
            style={{
              padding: '20px',
              background: '#e8f5e9',
              borderRadius: '8px',
              marginBottom: '20px',
            }}
          >
            <h3>
              <span role="img" aria-label="check mark">
                ✓
              </span>{' '}
              Benchmark Complete
            </h3>
            <p>
              <strong>Environment:</strong> {environmentLabel}
            </p>
            <p>
              <strong>Total Time:</strong> {totalTime.toFixed(2)}ms
            </p>
            <p>
              <strong>Best Query:</strong> {bestResult?.variantName}
            </p>
            <p>
              <strong>Best Time:</strong> {bestResult?.executionTime.toFixed(2)}
              ms
            </p>
            {!isNodeEnvironment && (
              <p
                style={{ fontSize: '0.9em', color: '#666', marginTop: '10px' }}
              >
                <span role="img" aria-label="tip">
                  💡
                </span>{' '}
                <strong>Tip:</strong> Run the same benchmark in the Node
                environment to compare native vs WASM performance!
              </p>
            )}
            {isNodeEnvironment && (
              <p
                style={{ fontSize: '0.9em', color: '#666', marginTop: '10px' }}
              >
                <span role="img" aria-label="tip">
                  💡
                </span>{' '}
                <strong>Note:</strong> Native Node.js typically runs 2-3x faster
                than Browser WASM for complex queries.
              </p>
            )}
          </div>

          {/* IN vs ANY Winner Summary Card - Only in comparative mode */}
          {comparative &&
            (() => {
              // Get all value counts tested
              const valueCounts = [
                ...new Set(results.map((r) => r.valueCount)),
              ].sort((a, b) => a - b);

              // For each value count, determine winner
              const winsByValueCount = valueCounts.map((count) => {
                const resultsAtCount = results.filter(
                  (r) => r.valueCount === count
                );

                // Get IN and ANY results
                const inResults = resultsAtCount.filter(
                  (r) =>
                    r.optimizations.includes('IN Operator') ||
                    (r.optimizationType.includes('IN') &&
                      !r.optimizationType.includes('ANY'))
                );
                const anyResults = resultsAtCount.filter(
                  (r) =>
                    r.optimizations.includes('ANY Operator') ||
                    r.optimizationType.includes('ANY')
                );

                if (inResults.length === 0 || anyResults.length === 0) {
                  return {
                    count,
                    winner: 'N/A',
                    inAvg: 0,
                    anyAvg: 0,
                    difference: 0,
                  };
                }

                // Calculate average for each
                const inAvg =
                  inResults.reduce((sum, r) => sum + r.executionTime, 0) /
                  inResults.length;
                const anyAvg =
                  anyResults.reduce((sum, r) => sum + r.executionTime, 0) /
                  anyResults.length;

                const difference = Math.abs(inAvg - anyAvg);
                const percentDiff =
                  (difference / Math.max(inAvg, anyAvg)) * 100;

                // Determine winner (with 5% tie threshold)
                let winner: 'IN' | 'ANY' | 'TIE';
                if (percentDiff < 5) {
                  winner = 'TIE';
                } else if (inAvg < anyAvg) {
                  winner = 'IN';
                } else {
                  winner = 'ANY';
                }

                return {
                  count,
                  winner,
                  inAvg,
                  anyAvg,
                  difference: percentDiff,
                };
              });

              const inWins = winsByValueCount.filter(
                (w) => w.winner === 'IN'
              ).length;
              const anyWins = winsByValueCount.filter(
                (w) => w.winner === 'ANY'
              ).length;
              const ties = winsByValueCount.filter(
                (w) => w.winner === 'TIE'
              ).length;

              // Determine overall winner
              const overallWinner =
                inWins > anyWins ? 'IN' : anyWins > inWins ? 'ANY' : 'TIE';
              const winnerColor =
                overallWinner === 'IN'
                  ? '#4caf50'
                  : overallWinner === 'ANY'
                  ? '#2196f3'
                  : '#ff9800';

              return (
                <div
                  style={{
                    padding: '20px',
                    background: '#fff',
                    borderRadius: '8px',
                    marginBottom: '20px',
                    border: `3px solid ${winnerColor}`,
                    boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                  }}
                >
                  <h2
                    style={{
                      marginTop: 0,
                      marginBottom: '15px',
                      color: winnerColor,
                    }}
                  >
                    <span role="img" aria-label="trophy">
                      🏆
                    </span>{' '}
                    IN vs ANY Winner Summary
                  </h2>

                  <div
                    style={{
                      display: 'flex',
                      gap: '20px',
                      marginBottom: '20px',
                      flexWrap: 'wrap',
                    }}
                  >
                    <div
                      style={{
                        flex: 1,
                        minWidth: '200px',
                        padding: '15px',
                        background: '#4caf50',
                        color: 'white',
                        borderRadius: '8px',
                        textAlign: 'center',
                      }}
                    >
                      <div style={{ fontSize: '2em', fontWeight: 'bold' }}>
                        {inWins}
                      </div>
                      <div style={{ fontSize: '0.9em' }}>IN Operator Wins</div>
                    </div>

                    <div
                      style={{
                        flex: 1,
                        minWidth: '200px',
                        padding: '15px',
                        background: '#2196f3',
                        color: 'white',
                        borderRadius: '8px',
                        textAlign: 'center',
                      }}
                    >
                      <div style={{ fontSize: '2em', fontWeight: 'bold' }}>
                        {anyWins}
                      </div>
                      <div style={{ fontSize: '0.9em' }}>ANY Operator Wins</div>
                    </div>

                    <div
                      style={{
                        flex: 1,
                        minWidth: '200px',
                        padding: '15px',
                        background: '#ff9800',
                        color: 'white',
                        borderRadius: '8px',
                        textAlign: 'center',
                      }}
                    >
                      <div style={{ fontSize: '2em', fontWeight: 'bold' }}>
                        {ties}
                      </div>
                      <div style={{ fontSize: '0.9em' }}>
                        Ties (&lt;5% diff)
                      </div>
                    </div>
                  </div>

                  <h3 style={{ marginBottom: '10px' }}>
                    Breakdown by Value Count:
                  </h3>
                  <div
                    style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}
                  >
                    {winsByValueCount.map(({ count, winner, difference }) => {
                      const bgColor =
                        winner === 'IN'
                          ? '#e8f5e9'
                          : winner === 'ANY'
                          ? '#e3f2fd'
                          : '#fff3e0';
                      const borderColor =
                        winner === 'IN'
                          ? '#4caf50'
                          : winner === 'ANY'
                          ? '#2196f3'
                          : '#ff9800';
                      const textColor =
                        winner === 'IN'
                          ? '#2e7d32'
                          : winner === 'ANY'
                          ? '#1565c0'
                          : '#e65100';

                      return (
                        <div
                          key={count}
                          style={{
                            padding: '10px 15px',
                            background: bgColor,
                            border: `2px solid ${borderColor}`,
                            borderRadius: '6px',
                            minWidth: '120px',
                            textAlign: 'center',
                          }}
                        >
                          <div
                            style={{
                              fontSize: '1.2em',
                              fontWeight: 'bold',
                              color: textColor,
                            }}
                          >
                            {winner}
                          </div>
                          <div style={{ fontSize: '0.85em', color: '#666' }}>
                            at {count} values
                          </div>
                          <div
                            style={{
                              fontSize: '0.75em',
                              color: '#999',
                              marginTop: '4px',
                            }}
                          >
                            {difference.toFixed(1)}% diff
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  <div
                    style={{
                      marginTop: '20px',
                      padding: '15px',
                      background: winnerColor,
                      color: 'white',
                      borderRadius: '8px',
                      fontSize: '1.1em',
                      fontWeight: 'bold',
                      textAlign: 'center',
                    }}
                  >
                    {overallWinner === 'TIE'
                      ? '🤝 Overall: IN and ANY are equally performant!'
                      : `🏆 Overall Winner: ${overallWinner} Operator (${
                          overallWinner === 'IN' ? inWins : anyWins
                        } out of ${valueCounts.length} value counts)`}
                  </div>
                </div>
              );
            })()}

          <h2>
            <span role="img" aria-label="trophy">
              🏆
            </span>{' '}
            Query Optimization Results Matrix
          </h2>
          <p style={{ color: '#666', marginBottom: '15px' }}>
            Click on any optimization strategy to expand and see how it performs
            at different scales (5, 10, 100, 1000 values).
          </p>

          <div style={{ overflowX: 'auto' }}>
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
                marginBottom: '30px',
                background: 'white',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
              }}
            >
              <thead>
                <tr style={{ background: '#2c3e50', color: 'white' }}>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                      width: '40px',
                    }}
                  ></th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                      width: '70px',
                    }}
                  >
                    Rank
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'left',
                      border: '1px solid #ddd',
                      minWidth: '280px',
                    }}
                  >
                    Optimization Strategy
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                      width: '100px',
                    }}
                  >
                    Values
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      width: '140px',
                    }}
                  >
                    Median Time (ms)
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      width: '120px',
                    }}
                  >
                    Min / Max
                    <div
                      style={{
                        fontSize: '0.7em',
                        fontWeight: 'normal',
                        color: '#ccc',
                        marginTop: '2px',
                      }}
                    >
                      (click Compare)
                    </div>
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      width: '100px',
                    }}
                  >
                    Std Dev
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                      width: '100px',
                    }}
                  >
                    View SQL
                  </th>
                </tr>
              </thead>
              <tbody>
                {getGroupedResults().map((group, groupIndex) => {
                  const isExpanded = expandedRows.has(group.optimizationType);
                  const firstVariant = group.variants[0];
                  const baselineMedian = getGroupedResults()[0].avgOfAverages;
                  const groupImprovement =
                    groupIndex === 0
                      ? 0
                      : ((baselineMedian - group.avgOfAverages) /
                          baselineMedian) *
                        100;

                  return (
                    <React.Fragment key={group.optimizationType}>
                      <tr
                        onClick={() => toggleRow(group.optimizationType)}
                        style={{
                          background:
                            groupIndex % 2 === 0 ? '#e3f2fd' : '#bbdefb',
                          cursor: 'pointer',
                          fontWeight: 'bold',
                          borderTop: '2px solid #2196f3',
                        }}
                      >
                        <td
                          style={{
                            padding: '15px 12px',
                            textAlign: 'center',
                            border: '1px solid #ddd',
                          }}
                        >
                          {isExpanded ? '▼' : '▶'}
                        </td>
                        <td
                          style={{
                            padding: '15px 12px',
                            textAlign: 'center',
                            border: '1px solid #ddd',
                          }}
                        >
                          {getRankBadge(group.rank)}
                        </td>
                        <td
                          style={{
                            padding: '15px 12px',
                            border: '1px solid #ddd',
                          }}
                        >
                          {group.optimizationType}
                          {getImprovementBadge(groupImprovement)}
                          <div
                            style={{
                              fontSize: '0.85em',
                              fontWeight: 'normal',
                              marginTop: '5px',
                            }}
                          >
                            {getOptimizationBadges(firstVariant.optimizations)}
                          </div>
                        </td>
                        <td
                          colSpan={2}
                          style={{
                            padding: '15px 12px',
                            textAlign: 'right',
                            border: '1px solid #ddd',
                          }}
                        >
                          {group.avgOfAverages.toFixed(2)}
                          <div
                            style={{ fontSize: '0.75em', fontWeight: 'normal' }}
                          >
                            avg across all scales
                          </div>
                        </td>
                        <td
                          colSpan={3}
                          style={{
                            padding: '15px 12px',
                            border: '1px solid #ddd',
                            textAlign: 'center',
                            fontSize: '0.9em',
                          }}
                        >
                          Click to expand and see performance at different
                          scales
                        </td>
                      </tr>

                      {/* Child Rows - Different Value Counts */}
                      {isExpanded &&
                        group.variants.map((variant) => {
                          const scalabilityVariant = VARIANTS.find(
                            (v) => v.id === variant.variantId
                          );
                          const ranksAtScale = getRanksWithinScale(
                            variant.valueCount
                          );
                          const rank = ranksAtScale.get(variant.variantId) || 0;
                          const baselineAtScale = getBaselineByScale().get(
                            variant.valueCount
                          );
                          const improvement = baselineAtScale
                            ? ((baselineAtScale - variant.executionTime) /
                                baselineAtScale) *
                              100
                            : 0;

                          return (
                            <tr
                              key={variant.variantId}
                              style={{
                                background: 'white',
                                borderLeft: '4px solid #2196f3',
                              }}
                            >
                              <td
                                style={{
                                  padding: '10px',
                                  border: '1px solid #ddd',
                                }}
                              ></td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'center',
                                  border: '1px solid #ddd',
                                }}
                              >
                                {getRankBadge(rank, 'small')}
                              </td>
                              <td
                                style={{
                                  padding: '10px 12px 10px 30px',
                                  border: '1px solid #ddd',
                                  fontSize: '0.9em',
                                }}
                              >
                                <span style={{ color: '#666' }}>└─ </span>
                                with {variant.valueCount} values
                                {getImprovementBadge(improvement, 'small')}
                                <div
                                  style={{
                                    fontSize: '0.75em',
                                    color: '#999',
                                    marginTop: '4px',
                                  }}
                                >
                                  {(() => {
                                    // Get all strategies at this value count
                                    const allAtScale = results.filter(
                                      (r) => r.valueCount === variant.valueCount
                                    );
                                    const sorted = [...allAtScale].sort(
                                      (a, b) =>
                                        a.executionTime - b.executionTime
                                    );
                                    const fastest = sorted[0];
                                    const position =
                                      sorted.findIndex(
                                        (r) => r.variantId === variant.variantId
                                      ) + 1;

                                    if (position === 1) {
                                      return `⚡ Fastest at ${variant.valueCount} values`;
                                    } else {
                                      const diff =
                                        ((variant.executionTime -
                                          fastest.executionTime) /
                                          fastest.executionTime) *
                                        100;
                                      return `${position}/${sorted.length} at ${
                                        variant.valueCount
                                      } values (+${diff.toFixed(
                                        1
                                      )}% vs fastest)`;
                                    }
                                  })()}
                                </div>
                              </td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'center',
                                  border: '1px solid #ddd',
                                  fontSize: '0.8em',
                                  color: '#666',
                                }}
                              >
                                {variant.valueCount}
                              </td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'right',
                                  border: '1px solid #ddd',
                                  fontWeight: 'bold',
                                }}
                              >
                                {variant.executionTime.toFixed(2)}
                              </td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'right',
                                  border: '1px solid #ddd',
                                  fontSize: '0.85em',
                                  color: '#666',
                                }}
                              >
                                <div>
                                  {variant.minTime.toFixed(2)} /{' '}
                                  {variant.maxTime.toFixed(2)}
                                </div>
                                <details
                                  style={{
                                    cursor: 'pointer',
                                    marginTop: '4px',
                                  }}
                                >
                                  <summary
                                    style={{
                                      fontSize: '0.8em',
                                      color: '#2196f3',
                                      cursor: 'pointer',
                                    }}
                                  >
                                    Compare
                                  </summary>
                                  <div
                                    style={{
                                      position: 'absolute',
                                      zIndex: 1000,
                                      background: 'white',
                                      border: '2px solid #2196f3',
                                      borderRadius: '4px',
                                      padding: '10px',
                                      minWidth: '300px',
                                      boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                                      marginTop: '5px',
                                      right: '100px',
                                      fontSize: '0.85em',
                                    }}
                                  >
                                    <div
                                      style={{
                                        fontWeight: 'bold',
                                        marginBottom: '8px',
                                        borderBottom: '1px solid #ddd',
                                        paddingBottom: '5px',
                                      }}
                                    >
                                      All Strategies at {variant.valueCount}{' '}
                                      values
                                    </div>
                                    {(() => {
                                      const allAtScale = results.filter(
                                        (r) =>
                                          r.valueCount === variant.valueCount
                                      );
                                      const sorted = [...allAtScale].sort(
                                        (a, b) =>
                                          a.executionTime - b.executionTime
                                      );

                                      return sorted
                                        .slice(0, 5)
                                        .map((r, idx) => {
                                          const isCurrent =
                                            r.variantId === variant.variantId;
                                          return (
                                            <div
                                              key={r.variantId}
                                              style={{
                                                padding: '4px',
                                                background: isCurrent
                                                  ? '#e3f2fd'
                                                  : 'transparent',
                                                borderLeft: isCurrent
                                                  ? '3px solid #2196f3'
                                                  : 'none',
                                                marginBottom: '3px',
                                              }}
                                            >
                                              <div
                                                style={{
                                                  display: 'flex',
                                                  justifyContent:
                                                    'space-between',
                                                  alignItems: 'center',
                                                }}
                                              >
                                                <span>
                                                  {idx + 1}.{' '}
                                                  {r.optimizationType}
                                                  {isCurrent && (
                                                    <strong> (this)</strong>
                                                  )}
                                                </span>
                                                <span
                                                  style={{ fontWeight: 'bold' }}
                                                >
                                                  {r.executionTime.toFixed(2)}ms
                                                </span>
                                              </div>
                                            </div>
                                          );
                                        });
                                    })()}
                                    {results.filter(
                                      (r) => r.valueCount === variant.valueCount
                                    ).length > 5 && (
                                      <div
                                        style={{
                                          fontSize: '0.8em',
                                          color: '#666',
                                          marginTop: '5px',
                                        }}
                                      >
                                        ... and{' '}
                                        {results.filter(
                                          (r) =>
                                            r.valueCount === variant.valueCount
                                        ).length - 5}{' '}
                                        more
                                      </div>
                                    )}
                                  </div>
                                </details>
                              </td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'right',
                                  border: '1px solid #ddd',
                                  fontSize: '0.85em',
                                  color: '#666',
                                }}
                              >
                                ±{variant.stdDev.toFixed(2)}
                              </td>
                              <td
                                style={{
                                  padding: '10px',
                                  textAlign: 'center',
                                  border: '1px solid #ddd',
                                }}
                              >
                                <details style={{ cursor: 'pointer' }}>
                                  <summary
                                    style={{
                                      padding: '4px 8px',
                                      background: '#2196f3',
                                      color: 'white',
                                      borderRadius: '4px',
                                      cursor: 'pointer',
                                      fontSize: '0.8em',
                                      listStyle: 'none',
                                    }}
                                  >
                                    SQL
                                  </summary>
                                  <div
                                    style={{
                                      position: 'absolute',
                                      zIndex: 1000,
                                      background: 'white',
                                      border: '2px solid #2196f3',
                                      borderRadius: '4px',
                                      padding: '15px',
                                      maxWidth: '600px',
                                      boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                                      marginTop: '5px',
                                      right: '20px',
                                    }}
                                  >
                                    <pre
                                      style={{
                                        background: '#f5f5f5',
                                        padding: '10px',
                                        borderRadius: '4px',
                                        overflow: 'auto',
                                        maxHeight: '400px',
                                        fontSize: '0.85em',
                                        margin: 0,
                                        whiteSpace: 'pre-wrap',
                                      }}
                                    >
                                      {scalabilityVariant?.query}
                                    </pre>
                                  </div>
                                </details>
                              </td>
                            </tr>
                          );
                        })}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Comparative Analysis Section - Only in comparative mode */}
          {comparative && results.length > 0 && (
            <div
              style={{
                padding: '20px',
                background: '#fff3e0',
                borderRadius: '8px',
                marginTop: '20px',
                border: '2px solid #ff9800',
              }}
            >
              <h2>
                <span role="img" aria-label="bar chart">
                  📊
                </span>{' '}
                IN vs ANY Comparative Analysis
              </h2>
              <p style={{ marginBottom: '15px', color: '#666' }}>
                Detailed performance comparison showing when IN outperforms ANY
                and vice versa across different query complexities and value
                counts.
              </p>

              {(() => {
                const analysis = getComparativeAnalysis(results);

                // Group by scenario
                const byScenario = new Map<string, typeof analysis>();
                analysis.forEach((item) => {
                  if (!byScenario.has(item.scenario)) {
                    byScenario.set(item.scenario, []);
                  }
                  byScenario.get(item.scenario)?.push(item);
                });

                return Array.from(byScenario.entries()).map(
                  ([scenario, items]) => {
                    const inWins = items.filter(
                      (i) => i.winner === 'IN'
                    ).length;
                    const anyWins = items.filter(
                      (i) => i.winner === 'ANY'
                    ).length;
                    const ties = items.filter((i) => i.winner === 'TIE').length;

                    return (
                      <div key={scenario} style={{ marginBottom: '20px' }}>
                        <h3
                          style={{
                            borderBottom: '2px solid #ff9800',
                            paddingBottom: '8px',
                          }}
                        >
                          {scenario}
                        </h3>
                        <div
                          style={{
                            display: 'flex',
                            gap: '15px',
                            marginBottom: '10px',
                          }}
                        >
                          <span
                            style={{
                              padding: '4px 12px',
                              background: '#4caf50',
                              color: 'white',
                              borderRadius: '4px',
                            }}
                          >
                            IN Wins: {inWins}
                          </span>
                          <span
                            style={{
                              padding: '4px 12px',
                              background: '#2196f3',
                              color: 'white',
                              borderRadius: '4px',
                            }}
                          >
                            ANY Wins: {anyWins}
                          </span>
                          <span
                            style={{
                              padding: '4px 12px',
                              background: '#9e9e9e',
                              color: 'white',
                              borderRadius: '4px',
                            }}
                          >
                            Ties: {ties}
                          </span>
                        </div>

                        <table
                          style={{
                            width: '100%',
                            borderCollapse: 'collapse',
                            fontSize: '0.9em',
                          }}
                        >
                          <thead style={{ background: '#f5f5f5' }}>
                            <tr>
                              <th
                                style={{
                                  padding: '8px',
                                  border: '1px solid #ddd',
                                }}
                              >
                                Values
                              </th>
                              <th
                                style={{
                                  padding: '8px',
                                  border: '1px solid #ddd',
                                }}
                              >
                                IN Time (ms)
                              </th>
                              <th
                                style={{
                                  padding: '8px',
                                  border: '1px solid #ddd',
                                }}
                              >
                                ANY Time (ms)
                              </th>
                              <th
                                style={{
                                  padding: '8px',
                                  border: '1px solid #ddd',
                                }}
                              >
                                Winner
                              </th>
                              <th
                                style={{
                                  padding: '8px',
                                  border: '1px solid #ddd',
                                }}
                              >
                                Advantage
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {items.map((item) => {
                              const winnerColor =
                                item.winner === 'IN'
                                  ? '#4caf50'
                                  : item.winner === 'ANY'
                                  ? '#2196f3'
                                  : '#9e9e9e';

                              return (
                                <tr key={item.valueCount}>
                                  <td
                                    style={{
                                      padding: '8px',
                                      border: '1px solid #ddd',
                                      textAlign: 'center',
                                    }}
                                  >
                                    {item.valueCount}
                                  </td>
                                  <td
                                    style={{
                                      padding: '8px',
                                      border: '1px solid #ddd',
                                      textAlign: 'right',
                                    }}
                                  >
                                    {item.inTime.toFixed(2)}
                                  </td>
                                  <td
                                    style={{
                                      padding: '8px',
                                      border: '1px solid #ddd',
                                      textAlign: 'right',
                                    }}
                                  >
                                    {item.anyTime.toFixed(2)}
                                  </td>
                                  <td
                                    style={{
                                      padding: '8px',
                                      border: '1px solid #ddd',
                                      textAlign: 'center',
                                      background: winnerColor,
                                      color: 'white',
                                      fontWeight: 'bold',
                                    }}
                                  >
                                    {item.winner}
                                  </td>
                                  <td
                                    style={{
                                      padding: '8px',
                                      border: '1px solid #ddd',
                                      textAlign: 'right',
                                    }}
                                  >
                                    {item.advantage.toFixed(1)}%
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    );
                  }
                );
              })()}
            </div>
          )}

          {bestResult && (
            <div
              style={{
                padding: '20px',
                background: '#c8e6c9',
                border: '3px solid #2e7d32',
                borderRadius: '8px',
                marginTop: '20px',
              }}
            >
              <h2>
                <span role="img" aria-label="trophy">
                  🏆
                </span>{' '}
                Recommended Query Pattern
              </h2>
              <p>
                <strong>Winner:</strong> {bestResult.variantName}
              </p>
              <p>
                <strong>Optimizations:</strong>{' '}
                {bestResult.optimizations.join(' + ') || 'None'}
              </p>
              <p>
                <strong>Performance:</strong>{' '}
                {bestResult.executionTime.toFixed(2)}
                ms (
                {(
                  ((results[0].executionTime - bestResult.executionTime) /
                    results[0].executionTime) *
                  100
                ).toFixed(1)}
                % faster than baseline)
              </p>
              <details style={{ marginTop: '15px' }}>
                <summary
                  style={{
                    cursor: 'pointer',
                    fontWeight: 'bold',
                    padding: '10px',
                    background: '#a5d6a7',
                    borderRadius: '4px',
                  }}
                >
                  <span role="img" aria-label="code">
                    💻
                  </span>{' '}
                  View Optimized Query SQL
                </summary>
                <pre
                  style={{
                    background: '#f5f5f5',
                    padding: '15px',
                    borderRadius: '4px',
                    overflow: 'auto',
                    marginTop: '10px',
                    fontSize: '0.9em',
                    lineHeight: '1.5',
                  }}
                >
                  {VARIANTS.find((v) => v.id === bestResult.variantId)?.query}
                </pre>
              </details>
            </div>
          )}
        </>
      )}
    </div>
  );
};
