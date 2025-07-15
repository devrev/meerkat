import { TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from './member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';

export const getAliasFromSchema = ({
  name,
  tableSchema,
  safe,
}: {
  name: string;
  tableSchema: TableSchema;
  safe?: boolean;
}): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  return constructAlias(name, findInSchema(field, tableSchema)?.alias, safe);
};

export const constructAlias = (
  name: string,
  alias?: string,
  safe?: boolean
): string => {
  if (alias) {
    if (safe) {
      // Alias may contain special characters or spaces, so we need to wrap in quotes.
      return `"${alias}"`;
    }
    return alias;
  }
  return memberKeyToSafeKey(name);
};
