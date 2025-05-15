export const generateViewQuery = (tableName: string, files: string[]) => {
  console.log(files, 'files');
  return `CREATE VIEW IF NOT EXISTS ${tableName} AS SELECT * FROM read_parquet(['${files.join(
    "','"
  )}']);`;
};
