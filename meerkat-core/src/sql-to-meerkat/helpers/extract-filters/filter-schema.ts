import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilterWithValues,
} from '../../../types/cube-types/query';
import { Dimension } from '../../../types/cube-types/table';
import { ExpressionClass, ParsedExpression } from '../../../types/duckdb-serialization-types';
import { getConstantTypeId } from '../../../utils/ast-constants';

/**
 * Type sets fetched dynamically from duckdb_types() catalog.
 * Used to classify filter column types from constant value AST nodes.
 */
export interface TypeSets {
  numeric: ReadonlySet<string>;   // e.g. INTEGER, BIGINT, DOUBLE, DECIMAL...
  datetime: ReadonlySet<string>;  // e.g. DATE, TIMESTAMP, INTERVAL...
}

/**
 * Infers Meerkat dimension type ('number' | 'time' | 'string') from a constant
 * expression's DuckDB type.
 *
 * Resolution:
 * 1. If the expression is a bare CONSTANT — check its value.type.id
 * 2. If it's a CAST — check the cast_type.id (target type of the cast)
 * 3. If neither matches — recurse into CAST child or default to 'string'
 *
 * This determines how cubeQueryToSQL renders the filter value
 * (quoted string vs unquoted number vs date literal).
 */
export function typeFromConstantExpr(
  expr: ParsedExpression,
  typeSets: TypeSets
): Dimension['type'] {
  // Try reading the constant's own type (e.g. INTEGER for literal 5)
  const typeId = getConstantTypeId(expr);
  if (!typeId) {
    // Not a bare constant — check if it's a CAST with a typed target
    if (expr.class === ExpressionClass.CAST) {
      const cast = expr as ParsedExpression & { cast_type?: { id?: string }; child?: ParsedExpression };
      // Use the CAST target type (e.g. CAST('2024-01-01' AS DATE) → DATE)
      const castId = cast.cast_type?.id?.toUpperCase() || '';
      if (typeSets.datetime.has(castId)) return 'time';
      if (typeSets.numeric.has(castId)) return 'number';
      // Target type unknown — try the inner constant
      return cast.child ? typeFromConstantExpr(cast.child, typeSets) : 'string';
    }
    return 'string';
  }
  // Match the constant's type against known categories
  const upper = typeId.toUpperCase();
  if (typeSets.numeric.has(upper)) return 'number';
  if (typeSets.datetime.has(upper)) return 'time';
  return 'string';
}

/**
 * Adds a dimension to the schema for filter columns not already in the SELECT list.
 *
 * When a WHERE clause references a column (e.g. WHERE priority > 3) that isn't
 * in the SELECT list, cubeQueryToSQL still needs it in the schema to build the
 * WHERE clause. This function creates a dimension entry for it.
 *
 * @param filter - The extracted filter containing the member reference
 * @param dimensions - Current dimensions in the schema
 * @param tableName - Primary table name (for prefix matching)
 * @param memberTypes - Type map populated during extraction (from AST constant types)
 * @returns New dimension(s) to add, or null if already exists
 */
export function ensureFilterColumnInSchema(
  filter: QueryFilterWithValues,
  dimensions: readonly Dimension[],
  tableName: string,
  memberTypes?: Record<string, Dimension['type']>
): Dimension[] | null {
  const prefix = `${tableName}.`;
  // Only process filters belonging to this table
  if (!filter.member.startsWith(prefix)) return null;
  const colName = filter.member.slice(prefix.length);
  // Skip if dimension already exists (from SELECT list)
  const exists = dimensions.some(
    (d) => d.name === colName || d.sql === colName
  );
  if (!exists) {
    // Use the inferred type from extraction, or default to 'string'
    const type = memberTypes?.[filter.member] || 'string';
    return [{ name: colName, sql: colName, type }];
  }
  return null;
}

/**
 * Walks an OR filter tree and ensures all referenced columns exist in the schema.
 * Delegates to ensureFilterColumnInSchema for each leaf filter.
 */
export function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const newDims: Dimension[] = [];
  collectFilterDimensions(orFilter, dimensions, tableName, newDims);
  return newDims.length > 0 ? newDims : null;
}

// Recursively walks a filter tree (or/and/leaf) collecting dimensions for
// any referenced columns not already in the schema.
function collectFilterDimensions(
  filter: QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter,
  dimensions: readonly Dimension[],
  tableName: string,
  newDims: Dimension[]
): void {
  if ('member' in filter) {
    // Leaf filter — check if its column needs a dimension
    const added = ensureFilterColumnInSchema(
      filter as QueryFilterWithValues,
      [...dimensions, ...newDims],
      tableName
    );
    if (added) newDims.push(...added);
  } else if ('or' in filter) {
    // OR node — recurse into each branch
    for (const child of (filter as LogicalOrFilter).or) {
      collectFilterDimensions(child as QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter, dimensions, tableName, newDims);
    }
  } else if ('and' in filter) {
    // AND node — recurse into each member
    for (const child of (filter as LogicalAndFilter).and) {
      collectFilterDimensions(
        child as QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter,
        dimensions,
        tableName,
        newDims
      );
    }
  }
}
