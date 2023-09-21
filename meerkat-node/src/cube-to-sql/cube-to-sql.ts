import { Query, TableSchema } from '@devrev/cube-types';
import { BASE_TABLE_NAME, cubeToDuckdbAST } from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const cube = {
  cube: 'select * from coolTable',
  measures: [
    {
      sql: 'base.test',
      type: 'string',
    },
  ],
  dimensions: [
    {
      sql: 'base.test_1',
      type: 'string',
    },
  ],
};

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
    `(${cube.cube})`
  );

  return replaceBaseTableName;
};
