import { TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from './member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';

/**
 * Configuration for query options.
 */
export interface QueryOptions {
  /**
   * When true, uses dot notation for aliases: "orders.customer_id"
   * When false, uses underscore notation: orders__customer_id
   */
  useDotNotation: boolean;
}

// ============================================================================
// NEW FLAG-AWARE API (recommended)
// ============================================================================

/**
 * Get alias for SQL string contexts (SELECT projections, etc.).
 * Always returns a properly quoted alias when needed.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param tableSchema - The table schema to look up custom aliases from
 * @param config - Configuration with useDotNotation flag
 * @returns The alias string, quoted when necessary
 *
 * @example
 * ```typescript
 * // With useDotNotation: false
 * getAliasForSQL("orders.total", schema, { useDotNotation: false }) // "orders__total"
 *
 * // With useDotNotation: true
 * getAliasForSQL("orders.total", schema, { useDotNotation: true }) // "\"orders.total\""
 * ```
 */
export const getAliasForSQL = (
  name: string,
  tableSchema: TableSchema,
  config: QueryOptions
): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  return constructAliasForSQL(
    name,
    findInSchema(field, tableSchema)?.alias,
    config
  );
};

/**
 * Get alias for AST contexts (DuckDB AST nodes, column references).
 * Returns unquoted alias - DuckDB handles quoting automatically.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param tableSchema - The table schema to look up custom aliases from
 * @param config - Configuration with useDotNotation flag
 * @returns The alias string, unquoted
 *
 * @example
 * ```typescript
 * // With useDotNotation: false
 * getAliasForAST("orders.total", schema, { useDotNotation: false }) // "orders__total"
 *
 * // With useDotNotation: true
 * getAliasForAST("orders.total", schema, { useDotNotation: true }) // "orders.total"
 * ```
 */
export const getAliasForAST = (
  name: string,
  tableSchema: TableSchema,
  config: QueryOptions
): string => {
  const [, field] = splitIntoDataSourceAndFields(name);
  return constructAliasForAST(
    name,
    findInSchema(field, tableSchema)?.alias,
    config
  );
};

/**
 * Construct alias for SQL string contexts directly from name and optional custom alias.
 * Always returns a properly quoted alias when needed.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param alias - Optional custom alias override
 * @param config - Configuration with useDotNotation flag
 * @returns The alias string, quoted when necessary
 */
export const constructAliasForSQL = (
  name: string,
  alias: string | undefined,
  config: QueryOptions
): string => {
  if (alias) {
    // Custom aliases always need quotes in SQL context (may contain spaces/special chars)
    return `"${alias}"`;
  }
  const safeKey = memberKeyToSafeKey(name, {
    useDotNotation: config.useDotNotation,
  });
  if (config.useDotNotation) {
    // With dot notation, we need to quote the safe key since it contains dots
    return `"${safeKey}"`;
  }
  // Underscore notation doesn't need quotes
  return safeKey;
};

/**
 * Construct alias for AST contexts directly from name and optional custom alias.
 * Returns unquoted alias - DuckDB handles quoting automatically.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param alias - Optional custom alias override
 * @param config - Configuration with useDotNotation flag
 * @returns The alias string, unquoted
 */
export const constructAliasForAST = (
  name: string,
  alias: string | undefined,
  config: QueryOptions
): string => {
  if (alias) {
    // Return custom alias without quotes - DuckDB will handle quoting
    return alias;
  }
  // Return safe key without quotes - DuckDB will handle quoting
  return memberKeyToSafeKey(name, { useDotNotation: config.useDotNotation });
};

// ============================================================================
// LEGACY API (deprecated, kept for backward compatibility)
// ============================================================================

/**
 * Get alias for a schema field.
 *
 * @param name - The member key (e.g., "orders.total_amount")
 * @param tableSchema - The table schema to look up custom aliases from
 * @param shouldWrapAliasWithQuotes - When true, wraps alias in quotes for SQL validity.
 *   Use `true` for SELECT projections where special characters need quoting.
 *   Use `false` for AST nodes (DuckDB auto-quotes) and internal schema references.
 *
 * @deprecated Use `getAliasForSQL` or `getAliasForAST` instead for clearer intent.
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
 *
 * @deprecated Use `constructAliasForSQL` or `constructAliasForAST` instead for clearer intent.
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
