import { validateDimensionQuery } from '@devrev/meerkat-browser';
import { useRef, useState } from 'react';
import { InstanceManager } from '../dbm-context/instance-manager';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { queries } from './dimension';

const validFunctions = [
  'any_value',
  'regexp_split_to_array',
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
  '-',
  '__internal_compress_string_hugeint',
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
  'list_value',
  'list_position',
  'monthname',
  '__internal_compress_string_utinyint',
  'enum_last',
  'not_like_escape',
  'even',
  'setseed',
  '__internal_decompress_integral_hugeint',
  'timezone',
  'regexp_replace',
  'to_binary',
  'gcd',
  'constant_or_null',
  'apply',
  'list_filter',
  'chr',
  'reverse',
  '__internal_compress_integral_uinteger',
  'prefix',
  'random',
  'nfc_normalize',
  'json_array_length',
  'log2',
  '>>',
  '*',
  'alias',
  'divide',
  'suffix',
  '__internal_compress_string_uinteger',
  'list_aggr',
  'vector_type',
  'multiply',
  'array_concat',
  'regexp_extract',
  'like_escape',
  'current_database',
  'stem',
  '//',
  '__internal_decompress_integral_smallint',
  'hex',
  'isoyear',
  'json_deserialize_sql',
  'json_type',
  'json_keys',
  'json_contains',
  'json_transform_strict',
  'json_structure',
  'json_merge_patch',
  'row_to_json',
  'to_json',
  'json_object',
  'json_extract_path_text',
  'json_extract_string',
  'json_extract_path',
  'json_extract',
  '~',
  'translate',
  '|',
  'concat',
  'yearweek',
  'year',
  'xor',
  'weekofyear',
  'list_indexof',
  'bit_length',
  'struct_extract',
  'array_contains',
  'ceiling',
  'list_distance',
  'dayname',
  'unicode',
  'typeof',
  'try_strptime',
  'encode',
  'to_months',
  'to_minutes',
  'to_days',
  'to_base64',
  '->>',
  'timezone_minute',
  'time_bucket',
  'regexp_full_match',
  'greatest',
  'struct_insert',
  'json_serialize_sql',
  'strptime',
  'json_array',
  'atan',
  'string_split',
  'strftime',
  'str_split_regex',
  'json_valid',
  'ilike_escape',
  '__internal_compress_string_usmallint',
  'least_common_multiple',
  'array_indexof',
  'list_element',
  'stats',
  '!__postfix',
  'dayofyear',
  'starts_with',
  'element_at',
  'union_value',
  'sin',
  '<<',
  'sign',
  'list_extract',
  'sha256',
  'enum_range',
  'array_distinct',
  'right',
  'list_reverse_sort',
  'damerau_levenshtein',
  'to_microseconds',
  'substring_grapheme',
  'replace',
  '&',
  'to_milliseconds',
  '%',
  'strlen',
  '<#>',
  'current_date',
  'combine',
  'isinf',
  'from_json_strict',
  '<=>',
  'rtrim',
  'ceil',
  'factorial',
  'length',
  'transaction_timestamp',
  'range',
  'radians',
  'century',
  'list_has',
  'pow',
  'isnan',
  'epoch_ns',
  'position',
  'tan',
  'array_reverse_sort',
  'now',
  'nextafter',
  'array_position',
  'union_extract',
  '/',
  'format',
  'epoch',
  'mismatches',
  'mod',
  'millisecond',
  'string_to_array',
  'millennium',
  'microsecond',
  'round',
  '@',
  '__internal_decompress_integral_uinteger',
  'md5_number',
  'abs',
  'md5',
  'list_resize',
  'map_values',
  'map_extract',
  'exp',
  'map_entries',
  'map_concat',
  'make_timestamp',
  'weekday',
  'make_time',
  'make_date',
  'array_aggr',
  'lpad',
  'set_bit',
  'log10',
  '__internal_decompress_integral_integer',
  'list_cat',
  'ln',
  'list_unique',
  'currval',
  'list_transform',
  'age',
  'list_sort',
  'list_pack',
  'list_dot_product',
  'list_apply',
  'aggregate',
  'lgamma',
  'month',
  '!~~',
  'left_grapheme',
  'least',
  'julian',
  'jaro_winkler_similarity',
  'isodow',
  'union_tag',
  'list_distinct',
  'isfinite',
  'today',
  'instr',
  'date_trunc',
  'concat_ws',
  'trim',
  'quarter',
  'jaccard',
  'to_years',
  'list_inner_product',
  'array_apply',
  'hash',
  'minute',
  'decade',
  'hamming',
  'greatest_common_divisor',
  'ends_with',
  'get_current_timestamp',
  'txid_current',
  'to_hours',
  'get_current_time',
  'timezone_hour',
  'bit_position',
  'generate_series',
  'signbit',
  'datepart',
  'levenshtein',
  '__internal_compress_integral_usmallint',
  'uuid',
  'gen_random_uuid',
  'string_split_regex',
  'to_seconds',
  'from_binary',
  'ltrim',
  '__internal_decompress_integral_usmallint',
  'array_to_json',
  'ucase',
  'from_base64',
  'rpad',
  '^',
  'format_bytes',
  'flatten',
  'filter',
  'power',
  'repeat',
  'era',
  'lcm',
  'datediff',
  'hour',
  'finalize',
  'epoch_ms',
  'right_grapheme',
  'enum_range_boundary',
  'degrees',
  '||',
  'dayofweek',
  'str_split',
  'datesub',
  'formatReadableDecimalSize',
  'date_diff',
  '<->',
  'array_has',
  'current_setting',
  'current_schemas',
  'map_keys',
  'json_quote',
  'array_slice',
  'current_query',
  'md5_number_lower',
  'map_from_entries',
  'list_cosine_similarity',
  'dayofmonth',
  'date_sub',
  'array_filter',
  'enum_code',
  'cot',
  'list_aggregate',
  'pi',
  'cbrt',
  'array_unique',
  'struct_pack',
  '**',
  'cardinality',
  'to_hex',
  'acos',
  'week',
  'to_timestamp',
  'trunc',
  'floor',
  'left',
  'bit_count',
  'current_schema',
  'bin',
  'ord',
  'bar',
  'epoch_us',
  'log',
  'array_aggregate',
  'enum_first',
  'array_extract',
  '__internal_compress_integral_ubigint',
  'lcase',
  'atan2',
  'bitstring',
  'gamma',
  'jaro_similarity',
  'asin',
  'split',
  'row',
  'array_transform',
  'array_sort',
  'list_slice',
  'decode',
  'cos',
  'datetrunc',
  'version',
  'unnest',
  'nullif',
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

      const promiseObj = validateDimensionQuery({
        connection: con,
        query: `select ${queries[i].sql_expression
          .replace(/'/g, "''")
          .replace('{', '(')
          .replace('}', ')')}`,
        validFunctions,
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
      {errorQueries.map((query) => {
        return <div>{query}</div>;
      })}
    </div>
  );
};
