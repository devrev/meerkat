// import { convertArrowTableToJSON } from '@devrev/meerkat-dbm';
// import { useState } from 'react';
// import { queries } from '../dimension';
// import { useDBM } from '../hooks/dbm-context';
// import { useClassicEffect } from '../hooks/use-classic-effect';

// const validFunctions = [
//   '^@',
//   'to_base',
//   'contains',
//   'length_grapheme',
//   'nextval',
//   '__internal_decompress_string',
//   'strpos',
//   'second',
//   '!~~*',
//   '~~*',
//   'add',
//   'octet_length',
//   '__internal_decompress_integral_bigint',
//   'array_length',
//   '~~',
//   'md5_number_upper',
//   'upper',
//   'map',
//   'array_resize',
//   'ascii',
//   'list_contains',
//   'day',
//   '~~~',
//   'get_bit',
//   'substring',
//   'subtract',
//   'array_cat',
//   '+',
//   'error',
//   'in_search_path',
//   'list_concat',
//   'lower',
//   'base64',
//   'strip_accents',
//   'editdist3',
//   'regexp_matches',
//   'json_transform',
//   '__internal_decompress_integral_ubigint',
//   'date_part',
//   'unbin',
//   'regexp_extract_all',
//   'from_json',
//   '-',
//   '__internal_compress_string_hugeint',
//   'from_hex',
//   'last_day',
//   'not_ilike_escape',
//   '__internal_compress_integral_utinyint',
//   'regexp_split_to_array',
//   'substr',
//   '__internal_compress_string_ubigint',
//   'unhex',
//   'len',
//   'sqrt',
//   'list_value',
//   'list_position',
//   'monthname',
//   '__internal_compress_string_utinyint',
//   'enum_last',
//   'not_like_escape',
//   'even',
//   'setseed',
//   '__internal_decompress_integral_hugeint',
//   'timezone',
//   'regexp_replace',
//   'to_binary',
//   'gcd',
//   'constant_or_null',
//   'apply',
//   'list_filter',
//   'chr',
//   'reverse',
//   '__internal_compress_integral_uinteger',
//   'prefix',
//   'random',
//   'nfc_normalize',
//   'json_array_length',
//   'log2',
//   '>>',
//   '*',
//   'alias',
//   'divide',
//   'suffix',
//   '__internal_compress_string_uinteger',
//   'list_aggr',
//   'vector_type',
//   'multiply',
//   'array_concat',
//   'regexp_extract',
//   'like_escape',
//   'current_database',
//   'stem',
//   '//',
//   '__internal_decompress_integral_smallint',
//   'hex',
//   'isoyear',
//   'json_deserialize_sql',
//   'json_type',
//   'json_keys',
//   'json_contains',
//   'json_transform_strict',
//   'json_structure',
//   'json_merge_patch',
//   'row_to_json',
//   'to_json',
//   'json_object',
//   'json_extract_path_text',
//   'json_extract_string',
//   'json_extract_path',
//   'json_extract',
//   '~',
//   'translate',
//   '|',
//   'concat',
//   'yearweek',
//   'year',
//   'xor',
//   'weekofyear',
//   'list_indexof',
//   'bit_length',
//   'struct_extract',
//   'array_contains',
//   'ceiling',
//   'list_distance',
//   'dayname',
//   'unicode',
//   'typeof',
//   'try_strptime',
//   'encode',
//   'to_months',
//   'to_minutes',
//   'to_days',
//   'to_base64',
//   '->>',
//   'timezone_minute',
//   'time_bucket',
//   'regexp_full_match',
//   'greatest',
//   'struct_insert',
//   'json_serialize_sql',
//   'strptime',
//   'json_array',
//   'atan',
//   'string_split',
//   'strftime',
//   'str_split_regex',
//   'json_valid',
//   'ilike_escape',
//   '__internal_compress_string_usmallint',
//   'least_common_multiple',
//   'array_indexof',
//   'list_element',
//   'stats',
//   '!__postfix',
//   'dayofyear',
//   'starts_with',
//   'element_at',
//   'union_value',
//   'sin',
//   '<<',
//   'sign',
//   'list_extract',
//   'sha256',
//   'enum_range',
//   'array_distinct',
//   'right',
//   'list_reverse_sort',
//   'damerau_levenshtein',
//   'to_microseconds',
//   'substring_grapheme',
//   'replace',
//   '&',
//   'to_milliseconds',
//   '%',
//   'strlen',
//   '<#>',
//   'current_date',
//   'combine',
//   'isinf',
//   'from_json_strict',
//   '<=>',
//   'rtrim',
//   'ceil',
//   'factorial',
//   'length',
//   'transaction_timestamp',
//   'range',
//   'radians',
//   'century',
//   'list_has',
//   'pow',
//   'isnan',
//   'epoch_ns',
//   'position',
//   'tan',
//   'array_reverse_sort',
//   'now',
//   'nextafter',
//   'array_position',
//   'union_extract',
//   '/',
//   'format',
//   'epoch',
//   'mismatches',
//   'mod',
//   'millisecond',
//   'string_to_array',
//   'millennium',
//   'microsecond',
//   'round',
//   '@',
//   '__internal_decompress_integral_uinteger',
//   'md5_number',
//   'abs',
//   'md5',
//   'list_resize',
//   'map_values',
//   'map_extract',
//   'exp',
//   'map_entries',
//   'map_concat',
//   'make_timestamp',
//   'weekday',
//   'make_time',
//   'make_date',
//   'array_aggr',
//   'lpad',
//   'set_bit',
//   'log10',
//   '__internal_decompress_integral_integer',
//   'list_cat',
//   'ln',
//   'list_unique',
//   'currval',
//   'list_transform',
//   'age',
//   'list_sort',
//   'list_pack',
//   'list_dot_product',
//   'list_apply',
//   'aggregate',
//   'lgamma',
//   'month',
//   '!~~',
//   'left_grapheme',
//   'least',
//   'julian',
//   'jaro_winkler_similarity',
//   'isodow',
//   'union_tag',
//   'list_distinct',
//   'isfinite',
//   'today',
//   'instr',
//   'date_trunc',
//   'concat_ws',
//   'trim',
//   'quarter',
//   'jaccard',
//   'to_years',
//   'list_inner_product',
//   'array_apply',
//   'hash',
//   'minute',
//   'decade',
//   'hamming',
//   'greatest_common_divisor',
//   'ends_with',
//   'get_current_timestamp',
//   'txid_current',
//   'to_hours',
//   'get_current_time',
//   'timezone_hour',
//   'bit_position',
//   'generate_series',
//   'signbit',
//   'datepart',
//   'levenshtein',
//   '__internal_compress_integral_usmallint',
//   'uuid',
//   'gen_random_uuid',
//   'string_split_regex',
//   'to_seconds',
//   'from_binary',
//   'ltrim',
//   '__internal_decompress_integral_usmallint',
//   'array_to_json',
//   'ucase',
//   'from_base64',
//   'rpad',
//   '^',
//   'format_bytes',
//   'flatten',
//   'filter',
//   'power',
//   'repeat',
//   'era',
//   'lcm',
//   'datediff',
//   'hour',
//   'finalize',
//   'epoch_ms',
//   'right_grapheme',
//   'enum_range_boundary',
//   'degrees',
//   '||',
//   'dayofweek',
//   'str_split',
//   'datesub',
//   'formatReadableDecimalSize',
//   'date_diff',
//   '<->',
//   'array_has',
//   'current_setting',
//   'current_schemas',
//   'map_keys',
//   'json_quote',
//   'array_slice',
//   'current_query',
//   'md5_number_lower',
//   'map_from_entries',
//   'list_cosine_similarity',
//   'dayofmonth',
//   'date_sub',
//   'array_filter',
//   'enum_code',
//   'cot',
//   'list_aggregate',
//   'pi',
//   'cbrt',
//   'array_unique',
//   'struct_pack',
//   '**',
//   'cardinality',
//   'to_hex',
//   'acos',
//   'week',
//   'to_timestamp',
//   'trunc',
//   'floor',
//   'left',
//   'bit_count',
//   'current_schema',
//   'bin',
//   'ord',
//   'bar',
//   'epoch_us',
//   'log',
//   'array_aggregate',
//   'enum_first',
//   'array_extract',
//   '__internal_compress_integral_ubigint',
//   'lcase',
//   'atan2',
//   'bitstring',
//   'gamma',
//   'jaro_similarity',
//   'asin',
//   'split',
//   'row',
//   'array_transform',
//   'array_sort',
//   'list_slice',
//   'decode',
//   'cos',
//   'datetrunc',
//   'version',
//   'unnest',
//   'nullif',
// ];

// function isValidFunction(node) {
//   if (
//     node.type === 'COLUMN_REF' ||
//     node.type === 'CONSTANT' ||
//     node.type === 'VALUE_CONSTANT'
//   ) {
//     return true;
//   } else if (node.type === 'OPERATOR_CAST') {
//     return isValidFunction(node.child);
//   } else if (node.type === 'OPERATOR_COALESCE') {
//     return node.children.every(isValidFunction);
//   } else if (
//     node.type === 'FUNCTION' &&
//     validFunctions.includes(node.function_name)
//   ) {
//     return true && node.children.every(isValidFunction);
//   } else if (node.type === 'CASE_EXPR') {
//     return (
//       node.case_checks.every((caseCheck) =>
//         isValidFunction(caseCheck.then_expr)
//       ) && isValidFunction(node.else_expr)
//     );
//   }
// }

// const validAggregations = [
//   'function_name',
//   'entropy',
//   'avg',
//   'bool_and',
//   'quantile_disc',
//   'covar_pop',
//   'covar_samp',
//   'max_by',
//   'count',
//   'array_agg',
//   'favg',
//   'quantile_cont',
//   'first',
//   'bit_and',
//   'min_by',
//   'string_agg',
//   'argmin',
//   'mean',
//   'histogram',
//   'skewness',
//   'arg_max',
//   'list',
//   'variance',
//   'mad',
//   'regr_syy',
//   'mode',
//   'bit_or',
//   'sumkahan',
//   'approx_quantile',
//   'kahan_sum',
//   'quantile',
//   'regr_avgx',
//   'regr_avgy',
//   'regr_count',
//   'regr_intercept',
//   'regr_r2',
//   'regr_slope',
//   'regr_sxx',
//   'reservoir_quantile',
//   'sem',
//   'product',
//   'corr',
//   'bit_xor',
//   'stddev',
//   'stddev_pop',
//   'stddev_samp',
//   'sum',
//   'sum_no_overflow',
//   'var_pop',
//   'kurtosis',
//   'arg_min',
//   'var_samp',
//   'regr_sxy',
//   'arbitrary',
//   'any_value',
//   'last',
//   'count_star',
//   'bool_or',
//   'group_concat',
//   'min',
//   'bitstring_agg',
//   'max',
//   'argmax',
//   'fsum',
//   'median',
//   'approx_count_distinct',
// ];

// const validOperators = ['+', '-', '*', '/', '||'];

// function isValidAggregation(node, parentNode = null, hasAggregation = false) {
//   // Base cases for column references and constants
//   if (
//     node.type === 'COLUMN_REF' ||
//     node.type === 'CONSTANT' ||
//     node.type === 'VALUE_CONSTANT'
//   ) {
//     // Allow column references inside aggregation functions
//     return !!parentNode;
//   }

//   // Check for valid aggregation functions
//   if (node.type === 'FUNCTION' || node.type === 'WINDOW_AGGREGATE') {
//     if (node.function_name === 'count_star') {
//       return true;
//     }

//     if (validAggregations.includes(node.function_name)) {
//       // This is a valid aggregation function - verify its children don't contain nested aggregations
//       return node.children.some((child) =>
//         isValidAggregation(child, node, true)
//       );
//     }
//     // For non-aggregation functions
//     if (validFunctions.includes(node.function_name)) {
//       return node.children.some((child) => {
//         return (
//           isValidAggregation(child, node, hasAggregation) &&
//           (containsAggregation(child) || hasAggregation)
//         );
//       });
//     }
//   }

//   // Handle CASE expressions
//   if (node.type === 'CASE_EXPR') {
//     const checksValid = node.case_checks.every((caseCheck) => {
//       // WHEN conditions cannot contain aggregations
//       const whenValid = !containsAggregation(caseCheck.when_expr);
//       // THEN expressions must be valid aggregations or contain no aggregations
//       const thenValid =
//         isValidAggregation(caseCheck.then_expr, parentNode, hasAggregation) ||
//         !containsAggregation(caseCheck.then_expr);
//       return whenValid && thenValid;
//     });

//     const elseValid =
//       isValidAggregation(node.else_expr, parentNode, hasAggregation) ||
//       !containsAggregation(node.else_expr);

//     return checksValid && elseValid;
//   }

//   if (node.type === 'SUBQUERY') {
//     return node.subquery.node.select_list.every((node) => {
//       return isValidAggregation(node, parentNode, hasAggregation);
//     });
//   }

//   if (node.type === 'WINDOW_LAG') {
//     return node.children.every((node) => {
//       return isValidAggregation(node, parentNode, hasAggregation);
//     });
//   }

//   if (node.type === 'OPERATOR_CAST') {
//     return isValidAggregation(node.child, node, hasAggregation);
//   }

//   // Handle COALESCE
//   if (node.type === 'OPERATOR_COALESCE') {
//     return node.children.every(
//       (child) =>
//         isValidAggregation(child, parentNode, hasAggregation) ||
//         !containsAggregation(child)
//     );
//   }

//   return false;
// }

// function containsAggregation(node) {
//   if (!node) return false;

//   if (node.type === 'FUNCTION' || node.type === 'WINDOW_AGGREGATE') {
//     return (
//       validAggregations.includes(node.function_name) ||
//       node.children.some(containsAggregation)
//     );
//   }

//   if (node.type === 'CASE_EXPR') {
//     return (
//       node.case_checks.some(
//         (check) =>
//           containsAggregation(check.when_expr) ||
//           containsAggregation(check.then_expr)
//       ) || containsAggregation(node.else_expr)
//     );
//   }

//   if (
//     node.type === 'OPERATOR_COALESCE' ||
//     (node.type === 'FUNCTION' && validOperators.includes(node.function_name))
//   ) {
//     return node.children.some(containsAggregation);
//   }

//   if (node.type === 'OPERATOR_CAST') {
//     return containsAggregation(node.child);
//   }

//   if (node.type === 'WINDOW_LAG') {
//     return node.children.some(containsAggregation);
//   }

//   if (node.type === 'SUBQUERY') {
//     return node.subquery.node.select_list.every((node) => {
//       return containsAggregation(node);
//     });
//   }

//   return false;
// }

// function countChar(str: string, char: string): number {
//   return [...str].filter((c) => c === char).length;
// }

// export const QueryBenchmarking = () => {
//   const [output, setOutput] = useState<
//     {
//       queryName: string;
//       time: any;
//     }[]
//   >([]);
//   const [totalTime, setTotalTime] = useState<number>(0);
//   const { dbm } = useDBM();

//   const [errorQueries, setErrorQueries] = useState<string[]>([]);
//   const [myAssumption, setmyAssumption] = useState(0);
//   const [notMyAssumption, setNotMyAssumption] = useState(0);
//   const [errorCount, setErrorCount] = useState(0);

//   useClassicEffect(() => {
//     setTotalTime(0);

//     setOutput([]);
//     const promiseArr = [];
//     const start = performance.now();
//     for (let i = 0; i < queries.length; i++) {
//       // if (
//       //   (queries[i].sql_expression.startsWith('sum') ||
//       //     queries[i].sql_expression.startsWith('count') ||
//       //     queries[i].sql_expression.startsWith('avg') ||
//       //     queries[i].sql_expression.startsWith('min') ||
//       //     queries[i].sql_expression.startsWith('max') ||
//       //     queries[i].sql_expression.startsWith('SUM') ||
//       //     queries[i].sql_expression.startsWith('COUNT') ||
//       //     queries[i].sql_expression.startsWith('AVG') ||
//       //     queries[i].sql_expression.startsWith('MIN') ||
//       //     queries[i].sql_expression.startsWith('MAX')) &&
//       //   countChar(queries[i].sql_expression, '(') === 1 &&
//       //   queries[i].sql_expression.endsWith(')')
//       // ) {
//       //   continue;
//       // }

//       const eachQueryStart = performance.now();
//       const promiseObj = dbm
//         .queryWithTables({
//           query: `SELECT json_serializ e_sql('${queries[i].sql_expression}')`,
//           tables: [{ name: 'taxi' }, { name: 'taxijson' }],
//         })
//         .then((results) => {
//           const responseData = convertArrowTableToJSON(results);

//           console.log(responseData, 'responseData');
//           try {
//             const data = JSON.parse(
//               responseData[0][
//                 `json_serialize_sql('${queries[i].sql_expression}')`
//               ]
//             );
//             console.log(
//               'data',
//               data,
//               isValidFunction(data.statements[0].node.select_list[0])
//             );
//             if (data.error) {
//               console.log('errorda function_name', data, queries[i]);
//               setErrorCount((prev) => prev + 1);
//               setErrorQueries((prev) => [...prev, queries[i].sql_expression]);
//             } else if (
//               !data.error &&
//               data.statements[0].node.select_list.length === 1 &&
//               isValidFunction(data.statements[0].node.select_list[0])
//             ) {
//               // console.log(
//               //   ' .function_name PASS',

//               //   data.statements[0].node.select_list[0].function_name,
//               //   data,
//               //   queries[i]
//               // );
//               // setOutput((prev) => [
//               //   ...prev,
//               //   {
//               //     queryName: `Query ${i} ---->`,
//               //     time: responseData[0][
//               //       `json_serialize_sql('select ${queries[i].sql_expression}')`
//               //     ],
//               //   },
//               // ]);
//               setmyAssumption((prev) => prev + 1);
//             } else {
//               setNotMyAssumption((prev) => prev + 1);
//               console.log(
//                 ' .function_name FAIL',
//                 data.statements[0].node.select_list[0].function_name,

//                 data,
//                 queries[i]
//               );
//               // setOutput((prev) => [
//               //   ...prev,
//               //   {
//               //     queryName: `Query ${i} ---->`,
//               //     time: responseData[0][
//               //       `json_serialize_sql('select ${queries[i].sql_expression}')`
//               //     ],
//               //   },
//               // ]);
//             }
//           } catch (e) {
//             console.log(e, 'errorda function_name');
//             setErrorCount((prev) => prev + 1);
//           }
//         })
//         .catch((e) => {
//           setErrorCount((prev) => prev + 1);
//           console.log(e, 'errorda function_name');

//           console.log(e, 'errorda', queries[i]);
//         });

//       promiseArr.push(promiseObj);
//     }
//     Promise.all(promiseArr).then(() => {
//       const end = performance.now();
//       const time = end - start;
//       setTotalTime(time);
//     });
//   }, []);

//   return (
//     <div>
//       {output.map((o, i) => {
//         return (
//           <div
//             data-query={`${i}`}
//             key={o.queryName}
//             style={{ marginTop: '50px' }}
//           >
//             {o.queryName} : {JSON.stringify(o.time, null, 2)}
//           </div>
//         );
//       })}
//       {totalTime === 0 && (
//         <div>
//           <span>Query Running...</span>
//         </div>
//       )}
//       {totalTime > 0 && (
//         <div>
//           Total Time: <span id="total_time">{totalTime}</span>
//         </div>
//       )}
//       Total queies : {queries.length} <br />
//       My Assumption : {myAssumption} <br />
//       Not My Assumption : {notMyAssumption} <br />
//       Error Count : {errorCount}
//       {/* {errorQueries.map((query) => {
//         return <div>{query}</div>;
//       })} */}
//     </div>
//   );
// };
