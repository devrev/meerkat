export const generateViewQuery = (
  tableName: string,
  files: string[],
  useTable = false
) => {
  const targetType = useTable ? 'TABLE' : 'VIEW';

  return `CREATE ${targetType} IF NOT EXISTS ${tableName} AS SELECT * FROM read_parquet(['${files.join(
    "','"
  )}']);`;
};
