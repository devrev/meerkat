import { isValidDimensionQuery } from '@devrev/meerkat-browser';
import { useRef, useState } from 'react';
import { InstanceManager } from '../dbm-context/instance-manager';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { queries } from './dimension';

const validFunctions = [
  '^@',
  'to_base',
  'contains',
  'length_grapheme',
  'nextval',
  '__internal_decompress_string',
  'strpos',
  'second',
  '!~~*',
  '~~*',
  'add',
  'octet_length',
  '__internal_decompress_integral_bigint',
  'array_length',
  '~~',
  'md5_number_upper',
  'upper',
  'map',
  'array_resize',
  'ascii',
  'list_contains',
  'day',
  '~~~',
  'get_bit',
  'substring',
  'subtract',
  'array_cat',
  '+',
  'error',
  'in_search_path',
  'list_concat',
  'lower',
  'base64',
  'strip_accents',
  'editdist3',
  'regexp_matches',
  'json_transform',
  '__internal_decompress_integral_ubigint',
  'date_part',
  'unbin',
  'regexp_extract_all',
  'from_json',
  'from_hex',
  'last_day',
  'not_ilike_escape',
  '__internal_compress_integral_utinyint',
  'regexp_split_to_array',
  'substr',
  '__internal_compress_string_ubigint',
  'unhex',
  'len',
  'sqrt',
];

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

      const promiseObj = isValidDimensionQuery({
        connection: con,
        query: `select ${queries[i].sql_expression}`,
      }).then((res) => {
        console.log(res, 'res');
        if (res.isValid) {
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
      {/* {output.map((o, i) => {
        return (
          <div
            data-query={`${i}`}
            key={o.queryName}
            style={{ marginTop: '50px' }}
          >
            {o.queryName} : {JSON.stringify(o.time, null, 2)}
          </div>
        );
      })} */}
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
