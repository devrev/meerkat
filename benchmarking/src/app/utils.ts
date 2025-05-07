export const generateViewQuery = (tableName: string, files: string[]) => {
  return `CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${files[0]}');`;
};
