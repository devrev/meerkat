import { duckdbExec } from './duckdb-exec';

export const nodeSQLToAST = async (sql: string) => {
  const queryOutput: any = await duckdbExec(sql);
  let ast: any;
  for (const key in queryOutput[0]) {
    if (Object.prototype.hasOwnProperty.call(queryOutput[0], key)) {
      ast = JSON.parse(queryOutput[0][key]);
      break;
    }
  }
  return ast;
};
