import {
  BASE_TABLE_NAME,
  Query,
  TableSchema,
  applyProjectionToSQLQuery,
  cubeToDuckdbAST,
} from '@devrev/meerkat-core';
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
    `(${tableSchema.sql}) AS ${tableSchema.name}`
  );

  /**
   * Add measures to the query
   */
  const measures = cubeQuery.measures;
  const dimensions = cubeQuery.dimensions || [];
  const finalQuery = applyProjectionToSQLQuery(
    dimensions,
    measures,
    tableSchema,
    replaceBaseTableName
  );

  return finalQuery;
};
