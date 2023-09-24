import { Query, TableSchema } from '@devrev/cube-types';
import { BASE_TABLE_NAME, cubeToDuckdbAST } from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export const cubeQueryToSQL = async (
  cubeQuery: Query,
  tableSchema: TableSchema
) => {
  const ast = cubeToDuckdbAST(cubeQuery, tableSchema);

  const queryTemp = `SELECT json_deserialize_sql('${JSON.stringify({
    statements: [ast],
  })}');`;

  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(queryTemp);

  const deserializeObj = queryOutput[0];
  const deserializeKey = Object.keys(deserializeObj)[0];
  const deserializeQuery = deserializeObj[deserializeKey];

  /**
   * Replace BASE_TABLE_NAME with cube query
   */
  const replaceBaseTableName = deserializeQuery.replace(
    BASE_TABLE_NAME,
    `(${tableSchema.cube})`
  );

  return replaceBaseTableName;
};

(async () => {
  // const version = await duckdbExec('DUCKDB_VERSION();');
  // console.log(version);
  const temp = await duckdbExec(
    ` SELECT json_deserialize_sql('{"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"STAR","type":"STAR","alias":"","relation_name":"","exclude_list":[],"replace_list":[],"columns":false,"expr":null}],"from_table":{"type":"BASE_TABLE","alias":"","sample":null,"schema_name":"","table_name":"REPLACE_BASE_TABLE","column_name_alias":[],"catalog_name":""},"where_clause":{"class":"COMPARISON","type":"COMPARE_EQUAL","alias":"","left":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["owned_by"]},"right":{"class":"CONSTANT","type":"VALUE_CONSTANT","alias":"","value":{"type":{"id":"DECIMAL","type_info":{"type":"DECIMAL_TYPE_INFO","alias":"","width":3,"scale":0}},"is_null":false,"value":null}}},"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null}}]}');`
  );
  console.log(temp);
})();
