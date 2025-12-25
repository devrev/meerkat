import { getNamespacedKey, memberKeyToSafeKey } from '../member-formatters';

export const meerkatPlaceholderReplacer = (
  sql: string,
  originalTableName: string
) => {
  const tableNameEncapsulationRegEx = /\{MEERKAT\}\.([a-zA-Z_][a-zA-Z0-9_]*)/g;
  return sql.replace(tableNameEncapsulationRegEx, (_, columnName) => {
    const namespacedKey = getNamespacedKey(originalTableName, columnName);
    // Use safe keys internally
    return `"${memberKeyToSafeKey(namespacedKey)}"`;
  });
};
