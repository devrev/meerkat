import { TableWiseFiles } from 'meerkat-dbm/src/types';
import { useMemo, useState } from 'react';
import { TEST_QUERIES } from '../constants';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

type Type =
  | 'raw'
  | 'memory'
  | 'indexed'
  | 'native'
  | 'parallel-memory'
  | 'parallel-indexed';

export const QueryBenchmarking = ({ type }: { type: Type }) => {
  const [output, setOutput] = useState<
    {
      queryName: string;
      time: number;
    }[]
  >([]);
  const [totalTime, setTotalTime] = useState<number>(0);
  const { dbm } = useDBM();

  const preQuery = useMemo(
    () =>
      async (tablesFileData: TableWiseFiles[]): Promise<void> => {
        if (type !== 'native' || !window.api) return;

        for (const table of tablesFileData) {
          const filePaths = await window.api?.getFilePathsForTable(
            table.tableName
          );
          console.log('filePaths', filePaths);
          if (!filePaths) {
            throw new Error('File paths not found');
          }

          await dbm.query(
            `CREATE TABLE IF NOT EXISTS ${
              table.tableName
            } AS SELECT * FROM read_parquet(['${filePaths.join("','")}']);`
          );
        }
      },
    [type, dbm]
  );

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
            preQuery: preQuery,
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
