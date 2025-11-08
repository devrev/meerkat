import { useEffect, useState } from 'react';
import {
  QueryBenchmarkResult,
  REAL_QUERY_VARIANTS,
} from '../constants-real-query-benchmark';
import { generateTestData } from '../generate-test-data';
import { useDBM } from '../hooks/dbm-context';

const ITERATIONS_PER_QUERY = 5; // Run each query 5 times for reliable averages

export const RealQueryBenchmark = () => {
  const { dbm, fileManagerType } = useDBM();
  const [results, setResults] = useState<QueryBenchmarkResult[]>([]);
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
          CREATE TABLE test_data AS 
          SELECT * FROM (VALUES ${values}) AS t(id, engineering_pod, type, subtype, year, 
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

  const runBenchmark = async () => {
    if (!dbm) return;

    setIsRunning(true);
    setResults([]);
    setCurrentQuery(0);
    const benchmarkResults: QueryBenchmarkResult[] = [];
    const startTime = performance.now();

    console.log('\n🚀 Starting Real-World Query Optimization Benchmark\n');
    console.log('Environment:', environmentLabel);
    console.log('Testing', REAL_QUERY_VARIANTS.length, 'query variants...');
    console.log(
      'Running each query',
      ITERATIONS_PER_QUERY,
      'times for reliable averages\n'
    );

    for (let i = 0; i < REAL_QUERY_VARIANTS.length; i++) {
      const variant = REAL_QUERY_VARIANTS[i];
      setCurrentQuery(i + 1);

      console.log(
        `\n📊 Testing Variant ${i + 1}/${REAL_QUERY_VARIANTS.length}`
      );
      console.log(`Name: ${variant.name}`);
      console.log(
        `Optimizations: ${variant.optimizations.join(', ') || 'None'}`
      );

      const runTimes: number[] = [];

      // Run query multiple times
      for (let iteration = 0; iteration < ITERATIONS_PER_QUERY; iteration++) {
        setCurrentIteration(iteration + 1);

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
      const avgTime = runTimes.reduce((a, b) => a + b, 0) / runTimes.length;
      const minTime = Math.min(...runTimes);
      const maxTime = Math.max(...runTimes);

      // Calculate standard deviation
      const squaredDiffs = runTimes.map((time) => Math.pow(time - avgTime, 2));
      const variance =
        squaredDiffs.reduce((a, b) => a + b, 0) / runTimes.length;
      const stdDev = Math.sqrt(variance);

      benchmarkResults.push({
        variantId: variant.id,
        variantName: variant.name,
        optimizations: variant.optimizations,
        executionTime: avgTime,
        minTime,
        maxTime,
        stdDev,
        allRuns: runTimes,
        rank: 0, // Will be calculated after all results are in
      });

      console.log(
        `✅ Average: ${avgTime.toFixed(2)}ms (min: ${minTime.toFixed(
          2
        )}ms, max: ${maxTime.toFixed(2)}ms, σ: ${stdDev.toFixed(2)}ms)`
      );
    }

    // Calculate rankings
    benchmarkResults.sort((a, b) => a.executionTime - b.executionTime);
    benchmarkResults.forEach((result, index) => {
      result.rank = index + 1;
    });

    // Sort back by variant order for display
    benchmarkResults.sort((a, b) => {
      const aIndex = REAL_QUERY_VARIANTS.findIndex((v) => v.id === a.variantId);
      const bIndex = REAL_QUERY_VARIANTS.findIndex((v) => v.id === b.variantId);
      return aIndex - bIndex;
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
  };

  const getBestResult = () => {
    if (results.length === 0) return null;
    return results.reduce((best, current) =>
      current.executionTime < best.executionTime ? current : best
    );
  };

  const getRowStyle = (rank: number, improvement: number, index: number) => {
    // Only highlight rows that are actually FASTER than baseline (positive improvement)
    const baseStyle = {
      background: index % 2 === 0 ? '#f9f9f9' : 'white',
    };

    if (improvement <= 0) {
      // If slower than baseline, no special highlighting
      return baseStyle;
    }

    // Only highlight if faster than baseline
    if (rank === 1)
      return {
        ...baseStyle,
        background: '#c8e6c9',
        fontWeight: 'bold',
        color: '#1b5e20',
      };
    if (rank === 2)
      return {
        ...baseStyle,
        background: '#fff9c4',
        fontWeight: 'bold',
        color: '#f57f17',
      };
    if (rank === 3)
      return {
        ...baseStyle,
        background: '#ffccbc',
        fontWeight: 'bold',
        color: '#bf360c',
      };
    return baseStyle;
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
        Testing {REAL_QUERY_VARIANTS.length} different optimization strategies
        on your production query pattern
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
          times for reliable average measurements
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
          ? `Running... (${currentQuery}/${REAL_QUERY_VARIANTS.length})`
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
            <strong>Query:</strong> {currentQuery} /{' '}
            {REAL_QUERY_VARIANTS.length}
          </p>
          <p>
            <strong>Iteration:</strong> {currentIteration} /{' '}
            {ITERATIONS_PER_QUERY}
          </p>
          <p style={{ fontSize: '0.9em', color: '#666' }}>
            Running each query {ITERATIONS_PER_QUERY} times for accurate
            averages...
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

          <h2>
            <span role="img" aria-label="trophy">
              🏆
            </span>{' '}
            Query Optimization Results Matrix
          </h2>
          <p style={{ color: '#666', marginBottom: '15px' }}>
            All query variants ranked by performance. Highlighted rows (Green,
            Yellow, Orange) indicate the top 3 queries that are FASTER than
            baseline.
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
                      width: '60px',
                    }}
                  >
                    Rank
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'left',
                      border: '1px solid #ddd',
                      minWidth: '200px',
                    }}
                  >
                    Query Variant
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'left',
                      border: '1px solid #ddd',
                      minWidth: '250px',
                    }}
                  >
                    Optimizations Applied
                  </th>
                  <th
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      width: '140px',
                    }}
                  >
                    Avg Time (ms)
                    <div
                      style={{
                        fontSize: '0.75em',
                        fontWeight: 'normal',
                        marginTop: '2px',
                      }}
                    >
                      ({ITERATIONS_PER_QUERY} runs)
                    </div>
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
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      width: '120px',
                    }}
                  >
                    vs Baseline
                    <div
                      style={{
                        fontSize: '0.75em',
                        fontWeight: 'normal',
                        marginTop: '2px',
                      }}
                    >
                      (+ faster / - slower)
                    </div>
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
                {results.map((result, index) => {
                  const variant = REAL_QUERY_VARIANTS.find(
                    (v) => v.id === result.variantId
                  );
                  const baseline = results[0];
                  const improvement =
                    ((baseline.executionTime - result.executionTime) /
                      baseline.executionTime) *
                    100;

                  return (
                    <tr
                      key={result.variantId}
                      style={getRowStyle(result.rank, improvement, index)}
                    >
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'center',
                          border: '1px solid #ddd',
                          fontSize: '1.2em',
                          fontWeight: 'bold',
                        }}
                      >
                        {result.rank === 1 && '🥇'}
                        {result.rank === 2 && '🥈'}
                        {result.rank === 3 && '🥉'}
                        {result.rank > 3 && result.rank}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          border: '1px solid #ddd',
                        }}
                      >
                        <strong>{result.variantName}</strong>
                        {variant && (
                          <div
                            style={{
                              fontSize: '0.85em',
                              color: '#666',
                              marginTop: '5px',
                            }}
                          >
                            {variant.description}
                          </div>
                        )}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          border: '1px solid #ddd',
                        }}
                      >
                        {getOptimizationBadges(result.optimizations)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          fontWeight: 'bold',
                          fontSize: '1.1em',
                        }}
                      >
                        {result.executionTime.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          fontSize: '0.9em',
                          color: '#666',
                        }}
                      >
                        {result.minTime.toFixed(2)} /{' '}
                        {result.maxTime.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          fontSize: '0.9em',
                          color: '#666',
                        }}
                      >
                        ±{result.stdDev.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          color: improvement > 0 ? '#2e7d32' : '#d32f2f',
                          fontWeight: 'bold',
                        }}
                      >
                        {improvement > 0 && (
                          <span role="img" aria-label="check">
                            ✓{' '}
                          </span>
                        )}
                        {improvement < 0 && (
                          <span role="img" aria-label="cross">
                            ✗{' '}
                          </span>
                        )}
                        {improvement > 0 ? '+' : ''}
                        {improvement.toFixed(1)}%
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'center',
                          border: '1px solid #ddd',
                        }}
                      >
                        <details style={{ cursor: 'pointer' }}>
                          <summary
                            style={{
                              padding: '6px 12px',
                              background: '#2196f3',
                              color: 'white',
                              borderRadius: '4px',
                              cursor: 'pointer',
                              fontWeight: 'bold',
                              fontSize: '0.9em',
                              listStyle: 'none',
                            }}
                          >
                            View SQL
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
                              {variant?.query}
                            </pre>
                          </div>
                        </details>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

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
                  {
                    REAL_QUERY_VARIANTS.find(
                      (v) => v.id === bestResult.variantId
                    )?.query
                  }
                </pre>
              </details>
            </div>
          )}
        </>
      )}
    </div>
  );
};
