import { useState } from 'react';
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
    const _testQueries = [
      'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
      "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
      'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
      'SELECT * as total_count FROM taxi.parquet ORDER BY bcf LIMIT 100',
      //   `
      //   WITH group_by_query AS (
      //     SELECT
      //         hvfhs_license_num,
      //         COUNT(*)
      //     FROM
      //         taxi.parquet
      //     GROUP BY
      //         hvfhs_license_num
      // ),

      // full_query AS (
      //     SELECT
      //         *
      //     FROM
      //         taxi.parquet
      // )

      // SELECT
      //     COUNT(*)
      // FROM
      //     group_by_query
      // LEFT JOIN
      //     full_query
      // ON
      //     group_by_query.hvfhs_license_num = full_query.hvfhs_license_num
      // LIMIT 1
      //   `,
    ];
    const testQueries = [..._testQueries, ..._testQueries, ..._testQueries];

    setOutput([]);
    const promiseArr = [];
    const start = performance.now();
    for (let i = 0; i < testQueries.length; i++) {
      const eachQueryStart = performance.now();

      const promiseObj = dbm
        .queryWithTableNames(testQueries[i], ['taxi'])
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
