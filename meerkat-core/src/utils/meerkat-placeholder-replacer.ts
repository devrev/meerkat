import { getNamespacedKey, getProjectionAlias } from '../member-formatters';
import { TableSchema } from '../types/cube-types';

export const meerkatPlaceholderReplacer = (
  sql: string,
  originalTableName: string,
  tableSchema: TableSchema,
  isDotDelimiterEnabled: boolean
) => {
  const tableNameEncapsulationRegEx = /\{MEERKAT\}\.([a-zA-Z_][a-zA-Z0-9_]*)/g;
  return sql.replace(tableNameEncapsulationRegEx, (_, columnName) => {
    return getProjectionAlias(
      getNamespacedKey(originalTableName, columnName),
      tableSchema,
      true,
      isDotDelimiterEnabled
    );
  });
};
