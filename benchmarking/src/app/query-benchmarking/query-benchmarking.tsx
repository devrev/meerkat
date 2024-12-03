import { convertArrowTableToJSON } from '@devrev/meerkat-dbm';
import { useState } from 'react';
import { queries } from '../dimension';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

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

  useClassicEffect(() => {
    setTotalTime(0);

    setOutput([]);
    const promiseArr = [];
    const start = performance.now();
    for (let i = 0; i < queries.length; i++) {
      const eachQueryStart = performance.now();
      const promiseObj = dbm
        .queryWithTables({
          query: `SELECT json_serialize_sql('${queries[i].sql_expression}')`,
          tables: [{ name: 'taxi' }, { name: 'taxijson' }],
        })
        .then((results) => {
          const responseData = convertArrowTableToJSON(results);

          console.log(responseData, 'responseData');
          try {
            const data = JSON.parse(
              responseData[0][
                `json_serialize_sql('${queries[i].sql_expression}')`
              ]
            );
            console.log(
              'data',
              data,
              isValidFunction(data.statements[0].node.select_list[0])
            );
            if (data.error) {
              console.log('errorda function_name', data, queries[i]);
              setErrorCount((prev) => prev + 1);
              setErrorQueries((prev) => [...prev, queries[i].sql_expression]);
            } else if (
              !data.error &&
              data.statements[0].node.select_list.length === 1 &&
              isValidFunction(data.statements[0].node.select_list[0])
            ) {
              // console.log(
              //   ' .function_name PASS',

              //   data.statements[0].node.select_list[0].function_name,
              //   data,
              //   queries[i]
              // );
              // setOutput((prev) => [
              //   ...prev,
              //   {
              //     queryName: `Query ${i} ---->`,
              //     time: responseData[0][
              //       `json_serialize_sql('select ${queries[i].sql_expression}')`
              //     ],
              //   },
              // ]);
              setmyAssumption((prev) => prev + 1);
            } else {
              setNotMyAssumption((prev) => prev + 1);
              console.log(
                ' .function_name FAIL',
                data.statements[0].node.select_list[0].function_name,

                data,
                queries[i]
              );
              // setOutput((prev) => [
              //   ...prev,
              //   {
              //     queryName: `Query ${i} ---->`,
              //     time: responseData[0][
              //       `json_serialize_sql('select ${queries[i].sql_expression}')`
              //     ],
              //   },
              // ]);
            }
          } catch (e) {
            console.log(e, 'errorda function_name');
            setErrorCount((prev) => prev + 1);
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
