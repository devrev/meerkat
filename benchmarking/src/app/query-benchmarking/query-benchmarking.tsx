import { TableWiseFiles } from 'meerkat-dbm/src/types';
import { useMemo, useState } from 'react';
import { TEST_QUERIES } from '../constants';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

type FileManagerType =
  | 'raw'
  | 'memory'
  | 'indexed'
  | 'native'
  | 'parallel-memory'
  | 'parallel-indexed';

export const QueryBenchmarking = ({ type }: { type: FileManagerType }) => {
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
        for (const table of tablesFileData) {
          let filePaths: string[] = [];

          if (type === 'native' && window.api) {
            filePaths = await window.api?.getFilePathsForTable(table.tableName);
          } else {
            filePaths = table.files.map((file) => file.fileName);
          }

          await dbm.query(
            `CREATE VIEW IF NOT EXISTS ${
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
          tables: [{ name: 'taxi_data' }, { name: 'taxi_json_data' }],
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
