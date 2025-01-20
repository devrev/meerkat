import { SelectStatement } from '@devrev/meerkat-core';
import { duckdbExec } from './duckdb-exec';

export interface ParsedSerialization {
  [key: string]: {
    error: boolean;
    statements: SelectStatement[];
  };
}

export const nodeSQLToSerialization = async (
  sql: string
): Promise<ParsedSerialization> => {
  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(sql);

  const parsedOutput: ParsedSerialization = {};

  for (const key in queryOutput[0]) {
    if (Object.prototype.hasOwnProperty.call(queryOutput[0], key)) {
      parsedOutput[key] = JSON.parse(queryOutput[0][key] as string) as {
        error: boolean;
        statements: SelectStatement[];
      };
    }
  }
  return parsedOutput;
};
