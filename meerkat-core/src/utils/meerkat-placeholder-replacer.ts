import { getAliasFromSchema, getNamespacedKey } from '../member-formatters';
import { MeerkatQueryOptions, TableSchema } from '../types/cube-types';

export const meerkatPlaceholderReplacer = (
  sql: string,
  originalTableName: string,
  tableSchema: TableSchema,
  options: MeerkatQueryOptions
) => {
  const tableNameEncapsulationRegEx = /\{MEERKAT\}\.([a-zA-Z_][a-zA-Z0-9_]*)/g;
  return sql.replace(tableNameEncapsulationRegEx, (_, columnName) => {
    return getAliasFromSchema({
      name: getNamespacedKey(originalTableName, columnName),
      tableSchema,
      shouldWrapAliasWithQuotes: true,
      isDotDelimiterEnabled: options.isDotDelimiterEnabled,
    });
  });
};
