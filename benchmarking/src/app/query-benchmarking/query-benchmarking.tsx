import { validateDimensionQuery } from '@devrev/meerkat-browser';
import React, { useState } from 'react';
import { InstanceManager } from '../dbm-context/instance-manager';
import { queries } from '../dimension';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

function getFormattedQuery(query: string) {
  return `SELECT ${query}`;
}

export const QueryBenchmarking = () => {
  const [output, setOutput] = useState<
    {
      queryName: string;
      time: any;
    }[]
  >([]);
  const [totalTime, setTotalTime] = useState<number>(0);
  const { dbm } = useDBM();

  const instanceManagerRef = React.useRef<InstanceManager>(
    new InstanceManager()
  );

  const [errorQueries, setErrorQueries] = useState<string[]>([]);
  const [myAssumption, setmyAssumption] = useState(0);
  const [notMyAssumption, setNotMyAssumption] = useState(0);
  const [errorCount, setErrorCount] = useState(0);

  useClassicEffect(async () => {
    setTotalTime(0);

    setOutput([]);
    const promiseArr = [];
    const start = performance.now();

    const db = await instanceManagerRef.current.getDB();

    const con = await db.connect();

    for (let i = 0; i < queries.length; i++) {
      const eachQueryStart = performance.now();
      const promiseObj = validateDimensionQuery({
        query: getFormattedQuery(queries[i].sql_expression),
        connection: con,
      })
        .then((results) => {
          if (results) {
            setmyAssumption((prev) => prev + 1);
          } else {
            setNotMyAssumption((prev) => prev + 1);
            console.log(results, 'errorda function_name');
          }
        })
        .catch((e) => {
          setErrorCount((prev) => prev + 1);
          console.log(e, 'errorda function_name');

          console.log(e, 'errorda', queries[i]);
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
          <div
            data-query={`${i}`}
            key={o.queryName}
            style={{ marginTop: '50px' }}
          >
            {o.queryName} : {JSON.stringify(o.time, null, 2)}
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
      Total queies : {queries.length} <br />
      My Assumption : {myAssumption} <br />
      Not My Assumption : {notMyAssumption} <br />
      Error Count : {errorCount}
      {/* {errorQueries.map((query) => {
        return <div>{query}</div>;
      })} */}
    </div>
  );
};
