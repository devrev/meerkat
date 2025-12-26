import { TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from './member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';

/**
 * Get alias for a schema field.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param tableSchema - The table schema to look up custom aliases from
 * @param shouldWrapAliasWithQuotes - When true, wraps alias in quotes for SQL validity.
 *   Use `true` for SELECT projections where special characters need quoting.
 *   Use `false` for AST nodes (DuckDB auto-quotes) and internal schema references.
 */
export const getAliasFromSchema = ({
  name,
  tableSchema,
  shouldWrapAliasWithQuotes,
}: {
  name: string;
  tableSchema: TableSchema;
  shouldWrapAliasWithQuotes: boolean;
}): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  return constructAlias({
    name,
    alias: findInSchema(field, tableSchema)?.alias,
    shouldWrapAliasWithQuotes,
  });
};

/**
 * Construct alias directly from name and optional custom alias.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param alias - Optional custom alias override
 * @param shouldWrapAliasWithQuotes - When true, wraps alias in quotes for SQL validity.
 *   Use `true` for SELECT projections where special characters need quoting.
 *   Use `false` for AST nodes (DuckDB auto-quotes) and internal schema references.
 */
export const constructAlias = ({
  name,
  alias,
  shouldWrapAliasWithQuotes,
}: {
  name: string;
  alias?: string;
  shouldWrapAliasWithQuotes: boolean;
}): string => {
  if (alias) {
    if (shouldWrapAliasWithQuotes) {
      // Alias may contain special characters or spaces, so we need to wrap in quotes.
      return `"${alias}"`;
    }
    return alias;
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
