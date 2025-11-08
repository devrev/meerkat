import { useState } from 'react';
import {
  FILTER_BENCHMARK_QUERIES,
  getQueryPairs,
} from '../constants-filter-benchmark';
import {
  OPTIMIZATION_PATTERNS,
  getOptimizationPairs,
} from '../constants-optimization-patterns';
import { generateTestData } from '../generate-test-data';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

interface BenchmarkResult {
  queryName: string;
  time: number;
  query: string;
  description: string;
}

interface ComparisonResult {
  testName: string;
  inTime: number;
  anyTime: number;
  improvement: string;
  winner: 'IN' | 'ANY' | 'TIE';
}

interface OptimizationComparisonResult {
  testName: string;
  slowTime: number;
  fastTime: number;
  improvement: string;
  timeSaved: number;
}

export const FilterOperatorBenchmark = () => {
  const [results, setResults] = useState<BenchmarkResult[]>([]);
  const [comparisons, setComparisons] = useState<ComparisonResult[]>([]);
  const [optimizationComparisons, setOptimizationComparisons] = useState<
    OptimizationComparisonResult[]
  >([]);
  const [totalTime, setTotalTime] = useState<number>(0);
  const [isRunning, setIsRunning] = useState<boolean>(false);
  const [currentQuery, setCurrentQuery] = useState<number>(0);
  const [loadingData, setLoadingData] = useState<boolean>(false);
  const [dataProgress, setDataProgress] = useState<string>('');
  const { dbm } = useDBM();

  const loadTestData = async (): Promise<void> => {
    setLoadingData(true);
    setDataProgress('Generating 100,000 rows of synthetic data...');

    // Generate 100,000 rows for realistic testing
    const testData = generateTestData(100000);
    console.log(`Generated ${testData.length} rows`);

    setDataProgress('Loading data into DuckDB...');

    // Create table and insert data in batches
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
    setIsRunning(true);
    setTotalTime(0);
    setResults([]);
    setComparisons([]);

    try {
      // Load test data first
      await loadTestData();
    } catch (error) {
      console.error('Error loading test data:', error);
      setIsRunning(false);
      return;
    }

    const start = performance.now();
    const benchmarkResults: BenchmarkResult[] = [];

    // Combine all queries
    const allQueries = [
      ...FILTER_BENCHMARK_QUERIES.map((q) => ({ ...q, type: 'IN_vs_ANY' })),
      ...OPTIMIZATION_PATTERNS.map((q) => ({ ...q, type: 'OPTIMIZATION' })),
    ];

    // Run each query sequentially to get accurate timing
    for (let i = 0; i < allQueries.length; i++) {
      setCurrentQuery(i + 1);
      const query = allQueries[i];
      const queryStart = performance.now();

      try {
        await dbm.query(query.query);

        const queryEnd = performance.now();
        const time = queryEnd - queryStart;

        const result: BenchmarkResult = {
          queryName: query.name,
          time,
          query: query.query,
          description: query.description,
        };

        benchmarkResults.push(result);
        setResults((prev) => [...prev, result]);
      } catch (error) {
        console.error(`Error running query ${query.name}:`, error);
      }
    }

    const end = performance.now();
    const totalTime = end - start;
    setTotalTime(totalTime);

    // Calculate comparisons
    const pairs = getQueryPairs();
    const comparisonResults: ComparisonResult[] = [];
    const consoleTableData: Array<{
      'Test Scenario': string;
      'IN (ms)': string;
      'ANY (ms)': string;
      'Difference (ms)': string;
      'Improvement (%)': string;
      Winner: string;
    }> = [];

    pairs.forEach((pair) => {
      const inResult = benchmarkResults.find(
        (r) => r.queryName === pair.in.name
      );
      const anyResult = benchmarkResults.find(
        (r) => r.queryName === pair.any.name
      );

      if (inResult && anyResult) {
        const inTime = inResult.time;
        const anyTime = anyResult.time;
        const diff = inTime - anyTime;
        const percentDiff = ((diff / inTime) * 100).toFixed(2);

        let winner: 'IN' | 'ANY' | 'TIE';
        let improvement: string;

        if (Math.abs(diff) < 1) {
          winner = 'TIE';
          improvement = 'Similar performance';
        } else if (diff > 0) {
          winner = 'ANY';
          improvement = `${percentDiff}% faster`;
        } else {
          winner = 'IN';
          improvement = `${Math.abs(parseFloat(percentDiff))}% faster`;
        }

        comparisonResults.push({
          testName: pair.in.description.replace('IN operator', 'Test'),
          inTime,
          anyTime,
          improvement,
          winner,
        });

        // Add to console table data
        consoleTableData.push({
          'Test Scenario': pair.category,
          'IN (ms)': inTime.toFixed(2),
          'ANY (ms)': anyTime.toFixed(2),
          'Difference (ms)': diff.toFixed(2),
          'Improvement (%)': percentDiff,
          Winner: winner,
        });
      }
    });

    setComparisons(comparisonResults);

    // Print comprehensive console table
    console.log('\n' + '='.repeat(100));
    console.log('🔥 IN vs ANY OPERATOR BENCHMARK RESULTS');
    console.log('='.repeat(100) + '\n');

    console.log('📊 DETAILED COMPARISON TABLE:');
    console.table(consoleTableData);

    // Calculate and print summary statistics
    const anyWins = comparisonResults.filter((c) => c.winner === 'ANY').length;
    const inWins = comparisonResults.filter((c) => c.winner === 'IN').length;
    const ties = comparisonResults.filter((c) => c.winner === 'TIE').length;

    const inTotal = comparisonResults.reduce((sum, c) => sum + c.inTime, 0);
    const anyTotal = comparisonResults.reduce((sum, c) => sum + c.anyTime, 0);
    const overallImprovement = (((inTotal - anyTotal) / inTotal) * 100).toFixed(
      2
    );

    console.log('\n📈 SUMMARY STATISTICS:');
    console.log('─'.repeat(100));
    console.log(`Total Scenarios Tested:     ${comparisonResults.length}`);
    console.log(
      `ANY Operator Wins:          ${anyWins} (${(
        (anyWins / comparisonResults.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log(
      `IN Operator Wins:           ${inWins} (${(
        (inWins / comparisonResults.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log(
      `Ties:                       ${ties} (${(
        (ties / comparisonResults.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log('─'.repeat(100));
    console.log(`Total IN Time:              ${inTotal.toFixed(2)} ms`);
    console.log(`Total ANY Time:             ${anyTotal.toFixed(2)} ms`);
    console.log(
      `Total Time Saved:           ${(inTotal - anyTotal).toFixed(2)} ms`
    );
    console.log(`Overall Improvement:        ${overallImprovement}%`);
    console.log('─'.repeat(100));

    // Performance by value count
    console.log('\n📊 PERFORMANCE BY VALUE COUNT:');
    const valueCountGroups = [
      { label: '5 values', data: consoleTableData[0] },
      { label: '10 values', data: consoleTableData[1] },
      { label: '20 values', data: consoleTableData[2] },
      { label: '30 values', data: consoleTableData[3] },
      { label: '50 values', data: consoleTableData[6] },
      { label: '100 values', data: consoleTableData[7] },
      { label: '500 values', data: consoleTableData[8] },
      { label: '1000 values', data: consoleTableData[9] },
    ].filter((g) => g.data);

    console.table(
      valueCountGroups.map((g) => ({
        'Value Count': g.label,
        'IN Time': g.data['IN (ms)'],
        'ANY Time': g.data['ANY (ms)'],
        Improvement: g.data['Improvement (%)'] + '%',
        Winner: g.data['Winner'],
      }))
    );

    // Recommendation
    console.log('\n💡 RECOMMENDATION:');
    console.log('─'.repeat(100));
    if (parseFloat(overallImprovement) > 10) {
      console.log('✅ STRONGLY RECOMMEND: Implement ANY operator');
      console.log(`   → ANY operator is ${overallImprovement}% faster overall`);
      console.log(
        `   → ${anyWins} out of ${comparisonResults.length} scenarios show improvement`
      );
      console.log(`   → Consider switching for filters with 10+ values`);
    } else if (parseFloat(overallImprovement) > 5) {
      console.log('⚠️  CONSIDER: ANY operator shows moderate improvement');
      console.log(`   → ANY operator is ${overallImprovement}% faster overall`);
      console.log(
        `   → Evaluate if ${overallImprovement}% improvement justifies code changes`
      );
    } else if (parseFloat(overallImprovement) > -5) {
      console.log('ℹ️  SIMILAR PERFORMANCE: Both operators perform comparably');
      console.log(
        `   → Overall difference: ${Math.abs(parseFloat(overallImprovement))}%`
      );
      console.log(`   → Keep current IN operator implementation`);
      console.log(
        `   → Focus on other optimizations (filter pushdown, deduplication)`
      );
    } else {
      console.log('❌ KEEP IN OPERATOR: IN performs better than ANY');
      console.log(
        `   → IN operator is ${Math.abs(
          parseFloat(overallImprovement)
        )}% faster`
      );
      console.log(`   → Do not switch to ANY operator`);
    }
    console.log('─'.repeat(100) + '\n');

    // Test optimization patterns
    console.log('\n' + '='.repeat(100));
    console.log('⚡ OPTIMIZATION PATTERN COMPARISONS');
    console.log('='.repeat(100) + '\n');

    const optimizationPairs = getOptimizationPairs();
    const optimizationTableData: Array<{
      Pattern: string;
      'Slow (ms)': string;
      'Fast (ms)': string;
      'Improvement (%)': string;
      'Time Saved (ms)': string;
    }> = [];
    const optimizationComparisonResults: OptimizationComparisonResult[] = [];

    optimizationPairs.forEach((pair) => {
      const slowResult = benchmarkResults.find(
        (r) => r.queryName === pair.slow.name
      );
      const fastResult = benchmarkResults.find(
        (r) => r.queryName === pair.fast.name
      );

      if (slowResult && fastResult) {
        const slowTime = slowResult.time;
        const fastTime = fastResult.time;
        const diff = slowTime - fastTime;
        const percentDiff = ((diff / slowTime) * 100).toFixed(2);

        optimizationTableData.push({
          Pattern: pair.category,
          'Slow (ms)': slowTime.toFixed(2),
          'Fast (ms)': fastTime.toFixed(2),
          'Improvement (%)': percentDiff,
          'Time Saved (ms)': diff.toFixed(2),
        });

        // Add to UI comparison results
        optimizationComparisonResults.push({
          testName: pair.category,
          slowTime,
          fastTime,
          improvement: `${percentDiff}% faster`,
          timeSaved: diff,
        });
      }
    });

    setOptimizationComparisons(optimizationComparisonResults);

    console.table(optimizationTableData);

    // Summary of optimization impact
    const totalSlowTime = optimizationTableData.reduce(
      (sum, row) => sum + parseFloat(row['Slow (ms)']),
      0
    );
    const totalFastTime = optimizationTableData.reduce(
      (sum, row) => sum + parseFloat(row['Fast (ms)']),
      0
    );
    const overallOptimization = (
      ((totalSlowTime - totalFastTime) / totalSlowTime) *
      100
    ).toFixed(2);

    console.log('\n🎯 OPTIMIZATION PATTERN SUMMARY:');
    console.log('─'.repeat(100));
    console.log(`Patterns Tested:            ${optimizationPairs.length}`);
    console.log(`Total Slow Time:            ${totalSlowTime.toFixed(2)} ms`);
    console.log(`Total Fast Time:            ${totalFastTime.toFixed(2)} ms`);
    console.log(
      `Total Time Saved:           ${(totalSlowTime - totalFastTime).toFixed(
        2
      )} ms`
    );
    console.log(`Overall Optimization:       ${overallOptimization}%`);
    console.log('─'.repeat(100));

    // Identify biggest wins
    const sortedByImpact = [...optimizationTableData].sort(
      (a, b) =>
        parseFloat(b['Improvement (%)']) - parseFloat(a['Improvement (%)'])
    );

    console.log('\n🏆 TOP 3 OPTIMIZATION OPPORTUNITIES:');
    sortedByImpact.slice(0, 3).forEach((row, index) => {
      console.log(
        `${index + 1}. ${row.Pattern}: ${
          row['Improvement (%)']
        }% improvement (${row['Time Saved (ms)']}ms saved)`
      );
    });
    console.log('─'.repeat(100) + '\n');

    setIsRunning(false);
    setCurrentQuery(0);
  };

  useClassicEffect(() => {
    runBenchmark();
  }, []);

  const getWinnerStyle = (winner: 'IN' | 'ANY' | 'TIE') => {
    if (winner === 'TIE') return { color: '#666' };
    return { color: '#2ecc71', fontWeight: 'bold' as const };
  };

  return (
    <div style={{ padding: '20px', fontFamily: 'monospace' }}>
      <h1>IN vs ANY Operator Performance Benchmark</h1>
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
          📊 Comprehensive Benchmark:{' '}
          {FILTER_BENCHMARK_QUERIES.length + OPTIMIZATION_PATTERNS.length} Test
          Scenarios
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>IN vs ANY:</strong> 5, 10, 20, 30, 50, 100, 500, 1000 values
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • <strong>Filter Optimization:</strong> Tests filter pushdown impact
          on query performance
        </p>
        <p style={{ margin: '0 0 5px 0' }}>
          • Query types: Simple, Aggregation, Complex, Subquery, CTE
        </p>
        <p style={{ margin: '0', fontWeight: 'bold', color: '#1976d2' }}>
          💡 Check browser console (F12) for detailed performance tables and
          recommendations!
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
          <h3>⏳ Loading Test Data</h3>
          <p>{dataProgress}</p>
          <p style={{ fontSize: '0.9em', color: '#666' }}>
            This only happens once at the start...
          </p>
        </div>
      )}

      {isRunning && !loadingData && (
        <div
          style={{
            padding: '20px',
            background: '#f0f0f0',
            borderRadius: '8px',
            marginBottom: '20px',
          }}
        >
          <h3>🔄 Running Benchmark</h3>
          <p>
            Progress: {currentQuery} / {FILTER_BENCHMARK_QUERIES.length}
          </p>
        </div>
      )}

      {!isRunning && totalTime > 0 && (
        <>
          <div
            style={{
              padding: '20px',
              background: '#e8f5e9',
              borderRadius: '8px',
              marginBottom: '20px',
            }}
          >
            <h3>✓ Benchmark Complete</h3>
            <p>
              <strong>Total Benchmark Time:</strong>{' '}
              <span id="total_time">{totalTime.toFixed(2)}ms</span>
            </p>
            <p>
              <strong>Queries Executed:</strong>{' '}
              {FILTER_BENCHMARK_QUERIES.length + OPTIMIZATION_PATTERNS.length}(
              {comparisons.length} IN/ANY comparisons +{' '}
              {getOptimizationPairs().length} pattern comparisons)
            </p>
            <p
              style={{
                marginTop: '10px',
                fontWeight: 'bold',
                color: '#2e7d32',
              }}
            >
              ✨ Detailed results logged to console - Press F12 to view!
            </p>
          </div>

          <h2>📊 IN vs ANY Comparison Results</h2>
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
              <tr style={{ background: '#3498db', color: 'white' }}>
                <th
                  style={{
                    padding: '12px',
                    textAlign: 'left',
                    border: '1px solid #ddd',
                  }}
                >
                  Test
                </th>
                <th
                  style={{
                    padding: '12px',
                    textAlign: 'right',
                    border: '1px solid #ddd',
                  }}
                >
                  IN (ms)
                </th>
                <th
                  style={{
                    padding: '12px',
                    textAlign: 'right',
                    border: '1px solid #ddd',
                  }}
                >
                  ANY (ms)
                </th>
                <th
                  style={{
                    padding: '12px',
                    textAlign: 'center',
                    border: '1px solid #ddd',
                  }}
                >
                  Improvement
                </th>
                <th
                  style={{
                    padding: '12px',
                    textAlign: 'center',
                    border: '1px solid #ddd',
                  }}
                >
                  Winner
                </th>
              </tr>
            </thead>
            <tbody>
              {comparisons.map((comp, index) => (
                <tr
                  key={index}
                  style={{
                    background: index % 2 === 0 ? '#f9f9f9' : 'white',
                    borderBottom: '1px solid #ddd',
                  }}
                >
                  <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                    {comp.testName}
                  </td>
                  <td
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      ...(comp.winner === 'IN' ? getWinnerStyle('ANY') : {}),
                    }}
                  >
                    {comp.inTime.toFixed(2)}
                  </td>
                  <td
                    style={{
                      padding: '12px',
                      textAlign: 'right',
                      border: '1px solid #ddd',
                      ...(comp.winner === 'ANY' ? getWinnerStyle('ANY') : {}),
                    }}
                  >
                    {comp.anyTime.toFixed(2)}
                  </td>
                  <td
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                    }}
                  >
                    {comp.improvement}
                  </td>
                  <td
                    style={{
                      padding: '12px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                      ...getWinnerStyle(comp.winner),
                    }}
                  >
                    {comp.winner}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {optimizationComparisons.length > 0 && (
            <>
              <h2 style={{ marginTop: '40px' }}>
                ⚡ Filter Optimization Results
              </h2>
              <p style={{ color: '#666', marginBottom: '15px' }}>
                Comparing query patterns with and without filter optimization
              </p>
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
                  <tr style={{ background: '#ff9800', color: 'white' }}>
                    <th
                      style={{
                        padding: '12px',
                        textAlign: 'left',
                        border: '1px solid #ddd',
                      }}
                    >
                      Optimization Pattern
                    </th>
                    <th
                      style={{
                        padding: '12px',
                        textAlign: 'right',
                        border: '1px solid #ddd',
                      }}
                    >
                      Without Optimization (ms)
                    </th>
                    <th
                      style={{
                        padding: '12px',
                        textAlign: 'right',
                        border: '1px solid #ddd',
                      }}
                    >
                      With Optimization (ms)
                    </th>
                    <th
                      style={{
                        padding: '12px',
                        textAlign: 'right',
                        border: '1px solid #ddd',
                      }}
                    >
                      Time Saved (ms)
                    </th>
                    <th
                      style={{
                        padding: '12px',
                        textAlign: 'center',
                        border: '1px solid #ddd',
                      }}
                    >
                      Improvement
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {optimizationComparisons.map((comp, index) => (
                    <tr
                      key={index}
                      style={{
                        background: index % 2 === 0 ? '#f9f9f9' : 'white',
                        borderBottom: '1px solid #ddd',
                      }}
                    >
                      <td style={{ padding: '12px', border: '1px solid #ddd' }}>
                        {comp.testName}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                        }}
                      >
                        {comp.slowTime.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          color: '#2ecc71',
                          fontWeight: 'bold',
                        }}
                      >
                        {comp.fastTime.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'right',
                          border: '1px solid #ddd',
                          color: '#2ecc71',
                          fontWeight: 'bold',
                        }}
                      >
                        {comp.timeSaved.toFixed(2)}
                      </td>
                      <td
                        style={{
                          padding: '12px',
                          textAlign: 'center',
                          border: '1px solid #ddd',
                          color: '#2ecc71',
                          fontWeight: 'bold',
                        }}
                      >
                        {comp.improvement}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}

          <h2>📋 Detailed Query Results</h2>
          <div style={{ marginBottom: '20px' }}>
            {results.map((result, index) => (
              <details
                key={index}
                style={{
                  marginBottom: '10px',
                  background: 'white',
                  padding: '10px',
                  borderRadius: '4px',
                  border: '1px solid #ddd',
                }}
              >
                <summary
                  style={{
                    cursor: 'pointer',
                    fontWeight: 'bold',
                    padding: '10px',
                  }}
                >
                  {result.queryName}: {result.time.toFixed(2)}ms -{' '}
                  {result.description}
                </summary>
                <pre
                  style={{
                    background: '#f5f5f5',
                    padding: '15px',
                    borderRadius: '4px',
                    overflow: 'auto',
                    marginTop: '10px',
                  }}
                >
                  {result.query}
                </pre>
              </details>
            ))}
          </div>
        </>
      )}

      {!isRunning && !loadingData && totalTime === 0 && (
        <div
          style={{
            padding: '20px',
            background: '#fff3cd',
            borderRadius: '8px',
          }}
        >
          <p>Initializing benchmark...</p>
        </div>
      )}
    </div>
  );
};
