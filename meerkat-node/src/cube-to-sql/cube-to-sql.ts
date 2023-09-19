// import { Query } from '@devrev/cube-types';
// import { SelectStatement } from '@devrev/duckdb-serialization-types';
// import { getTransformerFunction } from 'meerkat-core/src/cube-filter-transformer/factory';
// import { duckdbExec } from '../duckdb-exec';
// import {
//   ParsedSerialization,
//   nodeSQLToSerialization,
// } from '../node-sql-to-serialization';

import { Query } from '@devrev/cube-types';
import { MeerkatCore } from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

// const getStatement = (
//   serialization: ParsedSerialization
// ): SelectStatement | null => {
//   let statement: SelectStatement;

//   for (const key in serialization) {
//     if (Object.prototype.hasOwnProperty.call(serialization, key)) {
//       statement = serialization[key].statements[0];
//       return statement;
//     }
//   }
//   return null;
// };
// /**
//  * THIS IS A HACK JUST TO RUN STUFF, WE WILL CLEAN IT UP
//  * @param baseQuery
//  * @param cubeQuery
//  * @returns
//  */
// export const cubeQueryToSQL = async (
//   baseQuery: string,
//   cubeQuery: Pick<Query, 'filters'>
// ) => {
//   const serialization = await nodeSQLToSerialization(
//     `select json_serialize_sql('${baseQuery}')`
//   );

//   console.info('serialization done');
//   const statement = getStatement(serialization);

//   if (!statement) {
//     console.info('statement not found');
//     return null;
//   }

//   const cubeFilter = cubeQuery.filters?.[0] as any;

//   if (!cubeFilter) {
//     console.info('cubeFilter not found');
//     return null;
//   }

//   const transformer = getTransformerFunction(cubeFilter);
//   const duckdbWhere = transformer(cubeFilter);

//   // eslint-disable-next-line @typescript-eslint/ban-ts-comment
//   // @ts-ignore
//   statement.node['where_clause'] = duckdbWhere;

//   console.info(serialization);

//   let temp = '';
//   for (const key in serialization) {
//     if (Object.prototype.hasOwnProperty.call(serialization, key)) {
//       serialization[key].statements = [statement];
//       // eslint-disable-next-line @typescript-eslint/ban-ts-comment
//       //@ts-ignore
//       serialization[key] = JSON.stringify(serialization[key]);
//       temp = JSON.stringify(serialization[key]);
//     }
//   }

//   const queryTemp = `SELECT json_deserialize_sql('${JSON.stringify({
//     statements: [statement],
//   })}');`;

//   console.info(queryTemp);

//   const queryOutput = await duckdbExec<
//     {
//       [key: string]: string;
//     }[]
//   >(queryTemp);

//   let ouputSQL = '';

//   for (const key in queryOutput[0]) {
//     if (Object.prototype.hasOwnProperty.call(queryOutput[0], key)) {
//       ouputSQL = queryOutput[0][key];
//       break;
//     }
//   }

//   return ouputSQL;
// };

export const cubeQueryToSQL = async (cubeQuery: Query) => {
  const meerkatCore = new MeerkatCore();
  meerkatCore.addTableSchema({
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
  const ast = meerkatCore.cubeToDuckdbAST(cubeQuery);

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
