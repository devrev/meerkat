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
    for (let i = 0; i < TEST_QUERIES.length; i++) {
      const eachQueryStart = performance.now();

      const promiseObj = dbm
        .queryWithTables({
          query: TEST_QUERIES[i],
          tables: [{ name: 'taxi' }],
          options: {
            preQuery: async (tablesFileData) => {
              for (const table of tablesFileData) {
                dbm.query(
                  `CREATE TABLE IF NOT EXISTS ${
                    table.tableName
                  } AS SELECT * FROM read_parquet(['${table.files.map(
                    (file) => file.fileName
                  )}']);`
                );
              }
            },
          },
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
