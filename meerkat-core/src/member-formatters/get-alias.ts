import { TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from './member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';

export interface AliasContext {
  isAstIdentifier?: boolean;
  isTableSchemaAlias?: boolean;
}

const shouldUseSafeAlias = ({
  isAstIdentifier,
  isTableSchemaAlias,
}: AliasContext): boolean => {
  if (isAstIdentifier) {
    // Duckdb will automatically quote identifiers that contain special characters or spaces.
    // when converting the AST to SQL.
    return false;
  }
  if (isTableSchemaAlias) {
    // No need to wrap alias in quotes as part of table schema
    return false;
  }
  // For other cases such as directly using the alias in SQL queries,
  // we should make the alias safe to avoid issues with special characters or spaces.
  return true;
};

export const getAliasFromSchema = ({
  name,
  tableSchema,
  aliasContext,
}: {
  name: string;
  tableSchema: TableSchema;
  aliasContext: AliasContext;
}): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  return constructAlias({
    name,
    alias: findInSchema(field, tableSchema)?.alias,
    aliasContext,
  });
};

export const constructAlias = ({
  name,
  alias,
  aliasContext,
}: {
  name: string;
  alias?: string;
  aliasContext: AliasContext;
}): string => {
  if (alias) {
    if (shouldUseSafeAlias(aliasContext)) {
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
