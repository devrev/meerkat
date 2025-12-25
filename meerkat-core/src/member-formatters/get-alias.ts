import { TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from './member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';

/**
 * Gets a user-facing alias from the table schema, properly quoted for SQL.
 * This should only be used in the final projection layer.
 * Internal queries should use safe keys via memberKeyToSafeKey().
 */
export const getAliasFromSchema = ({
  name,
  tableSchema,
}: {
  name: string;
  tableSchema: TableSchema;
}): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  const schemaEntry = findInSchema(field, tableSchema);
  return constructAlias({
    name,
    alias: schemaEntry?.alias,
  });
};

/**
 * Constructs a quoted alias for use in final SQL projections.
 * If a user-defined alias exists, it will be quoted and returned.
 * Otherwise, returns a safe key version of the name.
 */
export const constructAlias = ({
  name,
  alias,
}: {
  name: string;
  alias?: string;
}): string => {
  if (alias) {
    // Alias may contain special characters or spaces, so we need to wrap in quotes.
    return `"${alias}"`;
  }
  return memberKeyToSafeKey(name);
};

/**
 * Creates a compound alias by joining two alias strings with " - ".
 * Used when a field resolves to multiple columns (e.g., "Owners - Display Name").
 *
 * @param baseAlias - The base field alias (e.g., "Owners")
 * @param resolutionAlias - The resolved field alias (e.g., "Display Name")
 * @returns The compound alias (e.g., "Owners - Display Name")
 *
 * @example
 * ```typescript
 * createCompoundAlias("Owners", "Display Name") // "Owners - Display Name"
 * createCompoundAlias("Tags", "Tag Name") // "Tags - Tag Name"
 * ```
 */
export const constructCompoundAlias = (
  baseAlias: string,
  resolutionAlias: string
): string => {
  return `${baseAlias} - ${resolutionAlias}`;
};
