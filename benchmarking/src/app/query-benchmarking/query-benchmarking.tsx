import { useState } from 'react';
import { TEST_QUERIES } from '../constants';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const QueryBenchmarking = () => {
  const [output, setOutput] = useState<
    {
      queryName: string;
      time: number;
    }[]
  >([]);
  const [totalTime, setTotalTime] = useState<number>(0);
  const { dbm } = useDBM();

  useClassicEffect(() => {
    setTotalTime(0);

    setOutput([]);
    const promiseArr = [];
    const start = performance.now();
    //Read the query params from the URL
    const urlParams = new URLSearchParams(window.location.search);
    const shardStr = urlParams.get('shard');
    const shard = shardStr ? parseInt(shardStr) : 1;
    //Split the TEST_QUERIES into 2 parts
    const splitIndex = Math.floor(TEST_QUERIES.length / 2);
    const testQueriesP1 = TEST_QUERIES.slice(0, splitIndex);
    const testQueriesP2 = TEST_QUERIES.slice(splitIndex);
    const testQueries = !shardStr
      ? TEST_QUERIES
      : shard === 1
      ? testQueriesP1
      : testQueriesP2;

    for (let i = 0; i < testQueries.length; i++) {
      const eachQueryStart = performance.now();

      const promiseObj = dbm
        .queryWithTables({
          query: testQueries[i],
          tables: [{ name: 'taxi' }, { name: 'taxijson' }],
        })
        .then((results) => {
          const end = performance.now();
          const time = end - eachQueryStart;
          setOutput((prev) => [
            ...prev,
            { queryName: `Query ${i} ---->`, time },
          ]);
        });

      promiseArr.push(promiseObj);
    }
    Promise.all(promiseArr).then(() => {
      const end = performance.now();
      const time = end - start;
      setTotalTime(time);
    });
  }, []);

  return (
    <div>
      {output.map((o, i) => {
        return (
          <div data-query={`${i}`} key={o.queryName}>
            {o.queryName} : {o.time}
          </div>
        );
      })}
      {totalTime === 0 && (
        <div>
          <span>Query Running...</span>
        </div>
      )}
      {totalTime > 0 && (
        <div>
          Total Time: <span id="total_time">{totalTime}</span>
        </div>
      )}
    </div>
  );
};
