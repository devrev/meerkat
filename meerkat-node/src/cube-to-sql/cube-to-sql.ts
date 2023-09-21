import { Query } from '@devrev/cube-types';
import { cubeToDuckdbAST } from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export const cubeQueryToSQL = async (cubeQuery: Query) => {
  const ast = cubeToDuckdbAST(cubeQuery, {
    cube: 'base',
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
  });

  const queryTemp = `SELECT json_deserialize_sql('${JSON.stringify({
    statements: [ast],
  })}');`;

  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(queryTemp);

  return queryOutput;
};
