import { duckdbExec } from "../../duckdb-exec";
const { 
    PARQUET_FILE_PATH,
    getOldQueries,
    getNewQueries
} = require('./constants')

const TABLE_NAME = 'benchmarking'

const RUNS_PER_QUERY = 20
const EXECUTION_INDEX = 5;

type RunInfo = {
    end: number;
    start: number;
}

const getParquetSQL = (parquetPath: string, TABLE_NAME: string) => `CREATE TABLE ${TABLE_NAME} AS SELECT * FROM read_parquet('${parquetPath}');` as const

const getAverageDuration = (results: { end: number, start: number }[]) => {
    let totalDuration = 0;
    results.forEach((result) => {
        const duration = result.end - result.start;
        totalDuration += duration;
    })
    const avgDuration = totalDuration / results.length;
    return avgDuration;
}

const getAverageImprovement = (oldRuns: RunInfo[], newRuns: RunInfo[]) => {
    const oldAvgDuration = getAverageDuration(oldRuns)
    const newAvgDuration = getAverageDuration(newRuns)
    // Calculate improvement
    const improvement = ((oldAvgDuration - newAvgDuration) / oldAvgDuration) * 100;
    return {
        oldAvgDuration,
        newAvgDuration,
        improvement
    }
}


const getPercentileItem = (runs: RunInfo[], percentile: number) => {
    const sortedRuns = runs.sort((a, b) => a.end - a.start - (b.end - b.start))
    const p90Index = Math.floor(runs.length * percentile)
    return sortedRuns[p90Index]
}

const getPercentileImprovement = (oldRuns: RunInfo[], newRuns: RunInfo[], percentile: number) => {
    const { end: oldEndP90, start: oldStartP90 } = getPercentileItem(oldRuns, percentile)
    const { end: newEndP90, start: newStartP90 } = getPercentileItem(newRuns, percentile)
    const oldDuration = oldEndP90 - oldStartP90
    const newDuration = newEndP90 - newStartP90
    const improvement = ((oldDuration - newDuration) / oldDuration) * 100;
    return {
        oldDuration,
        newDuration,
        improvement
    }
}

const runQueryWithTiming = async (sql: string, runs: number): Promise<RunInfo[]> => {
    const runsArray = new Array(runs).fill(undefined)
    const dataPromises = runsArray.map(async () => {
        const start = performance.now();
        await duckdbExec(sql);
        const end = performance.now();
        return {
            end,
            start
        }
    })
    const results = await Promise.all(dataPromises);
    return results

}


const benchmarkQueries = async (tableName: string, runsPerQuery: number) => {
    const oldQueries = getOldQueries(tableName);
    const newQueries = getNewQueries(tableName);
        
    const oldQuery = oldQueries[EXECUTION_INDEX];
    const newQuery = newQueries[EXECUTION_INDEX];

    console.log({ oldQuery, newQuery })
    // Run old query multiple times and get average
    const oldRuns = await runQueryWithTiming(oldQuery.sql, runsPerQuery);

    // Run new query multiple times and get average
    const newRuns = await runQueryWithTiming(newQuery.sql, runsPerQuery);

    const avgImprovement = getAverageImprovement(oldRuns, newRuns)
    const p50Improvement = getPercentileImprovement(oldRuns, newRuns, 0.5)
    const p90Improvement = getPercentileImprovement(oldRuns, newRuns, 0.9)

    
    return {
        avg: avgImprovement,
        p50: p50Improvement,
        p90: p90Improvement
    }

}



describe('benchmarking', () => {
    beforeAll(async () => {
        const sql = getParquetSQL(PARQUET_FILE_PATH, TABLE_NAME);
        await duckdbExec(sql);
    })

    it('should be able to run benchmarks', async () => {
        const result  = await benchmarkQueries(TABLE_NAME, RUNS_PER_QUERY)
        console.log(result)
    })
})

