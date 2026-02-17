import { getAliasForSQL } from '../member-formatters/get-alias';
import { getNamespacedKey } from '../member-formatters/get-namespaced-key';
import { TableSchema } from '../types/cube-types';

export const meerkatPlaceholderReplacer = (
  sql: string,
  originalTableName: string,
  tableSchema: TableSchema
) => {
  const tableNameEncapsulationRegEx = /\{MEERKAT\}\.([a-zA-Z_][a-zA-Z0-9_]*)/g;
  return sql.replace(tableNameEncapsulationRegEx, (_, columnName) => {
    return getAliasForSQL(
      getNamespacedKey(originalTableName, columnName),
      tableSchema
    );
  });
};
