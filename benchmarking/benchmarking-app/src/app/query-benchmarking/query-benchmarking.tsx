import React, { useState } from 'react';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const QueryBenchmarking = () => {
  const [output, setOutput] = useState<
    {
      queryName: string;
      time: number;
    }[]
  >([]);
  const { dbm } = useDBM();

  useClassicEffect(() => {
    const testQueries = [
      'SELECT * as total_count FROM taxi.parquet ORDER BY bcf LIMIT 100',
      'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
      "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
      'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
      `
      WITH group_by_query AS (
        SELECT 
            hvfhs_license_num, 
            COUNT(*) 
        FROM 
            taxi.parquet 
        GROUP BY 
            hvfhs_license_num
    ), 
    
    full_query AS (
        SELECT 
            * 
        FROM 
            taxi.parquet
    ) 
    
    SELECT 
        COUNT(*) 
    FROM 
        group_by_query 
    LEFT JOIN 
        full_query 
    ON 
        group_by_query.hvfhs_license_num = full_query.hvfhs_license_num 
    LIMIT 1
      `,
    ];

    setOutput([]);

    for (let i = 0; i < testQueries.length; i++) {
      const start = performance.now();

      dbm
        .queryWithTableNames(testQueries[i], ['taxi.parquet'])
        .then((results) => {
          const end = performance.now();
          const time = end - start;
          setOutput((prev) => [
            ...prev,
            { queryName: `Query ${i} ---->`, time },
          ]);
        });
    }
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
    </div>
  );
};
