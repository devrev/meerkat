import { validateMeasureQuery } from '@devrev/meerkat-browser';
import { useRef, useState } from 'react';
import { InstanceManager } from '../dbm-context/instance-manager';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { queries } from './dimension';

export const QueryBenchmarking = () => {
  const [output, setOutput] = useState<
    {
      queryName: string;
      time: any;
    }[]
  >([]);
  const [totalTime, setTotalTime] = useState<number>(0);
  const { dbm } = useDBM();

  const [errorQueries, setErrorQueries] = useState<string[]>([]);
  const [myAssumption, setmyAssumption] = useState(0);
  const [notMyAssumption, setNotMyAssumption] = useState(0);
  const [errorCount, setErrorCount] = useState(0);

  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());

  useClassicEffect(async () => {
    setTotalTime(0);

    setOutput([]);
    const promiseArr = [];
    const start = performance.now();

    const instanceManager = instanceManagerRef.current;

    const db = await instanceManager.getDB();
    const con = await db.connect();

    for (let i = 0; i < queries.length; i++) {
      const eachQueryStart = performance.now();
      const promiseObj = validateMeasureQuery({
        connection: con,
        query: `select ${queries[i].sql_expression
          .replace(/'/g, "''")
          .replace('{', '(')
          .replace('}', ')')}`,
      })
        .then((res) => {
          if (res) {
            setmyAssumption((prev) => prev + 1);
          } else {
            setNotMyAssumption((prev) => prev + 1);
            setOutput((prev) => [
              ...prev,
              {
                queryName: `Query ${i} ---->`,
                time: performance.now() - eachQueryStart,
              },
            ]);
          }
        })
        .catch((err) => {
          console.log(err);
          setErrorCount((prev) => prev + 1);
          setErrorQueries((prev) => [...prev, queries[i].sql_expression]);
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
