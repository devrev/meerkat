import { SelectStatement } from '@devrev/duckdb-serialization-types';
import { duckdbExec } from './duckdb-exec';

export const nodeSQLToSerialization = async (
  sql: string
): Promise<SelectStatement> => {
  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(sql);

  let ast: SelectStatement[] = [];
  for (const key in queryOutput[0]) {
    if (Object.prototype.hasOwnProperty.call(queryOutput[0], key)) {
      ast = JSON.parse(queryOutput[0][key]).statements;
      break;
    }
  }
  return ast[0];
};
