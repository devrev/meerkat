export const generateViewQuery = (tableName: string, files: string[]) => {
  return `CREATE VIEW IF NOT EXISTS ${tableName} AS SELECT * FROM read_parquet(['${files.join(
    "','"
  )}']);`;
};
