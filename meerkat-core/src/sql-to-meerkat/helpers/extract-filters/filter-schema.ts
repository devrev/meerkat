import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilterWithValues,
} from '../../../types/cube-types/query';
import { Dimension } from '../../../types/cube-types/table';
import { ExpressionClass, ParsedExpression } from '../../../types/duckdb-serialization-types';
import { getConstantTypeId } from '../../../utils/ast-constants';

export interface TypeSets {
  numeric: ReadonlySet<string>;
  datetime: ReadonlySet<string>;
}

// Infers Meerkat dimension type from a constant's DuckDB type ID.
// Uses dynamically-fetched type sets from duckdb_types() catalog.
export function typeFromConstantExpr(
  expr: ParsedExpression,
  typeSets: TypeSets
): Dimension['type'] {
  const typeId = getConstantTypeId(expr);
  if (!typeId) {
    if (expr.class === ExpressionClass.CAST) {
      const cast = expr as ParsedExpression & { cast_type?: { id?: string }; child?: ParsedExpression };
      const castId = cast.cast_type?.id?.toUpperCase() || '';
      if (typeSets.datetime.has(castId)) return 'time';
      if (typeSets.numeric.has(castId)) return 'number';
      return cast.child ? typeFromConstantExpr(cast.child, typeSets) : 'string';
    }
    return 'string';
  }
  const upper = typeId.toUpperCase();
  if (typeSets.numeric.has(upper)) return 'number';
  if (typeSets.datetime.has(upper)) return 'time';
  return 'string';
}

// Adds a dimension to the schema for filter columns not already in the SELECT list.
// This allows cubeQueryToSQL to reference the column when building the WHERE clause.
export function ensureFilterColumnInSchema(
  filter: QueryFilterWithValues,
  dimensions: readonly Dimension[],
  tableName: string,
  memberTypes?: Record<string, Dimension['type']>
): Dimension[] | null {
  const prefix = `${tableName}.`;
  if (!filter.member.startsWith(prefix)) return null;
  const colName = filter.member.slice(prefix.length);
  const exists = dimensions.some(
    (d) => d.name === colName || d.sql === colName
  );
  if (!exists) {
    const type = memberTypes?.[filter.member] || 'string';
    return [{ name: colName, sql: colName, type }];
  }
  return null;
}

export function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const newDims: Dimension[] = [];
  collectFilterDimensions(orFilter, dimensions, tableName, newDims);
  return newDims.length > 0 ? newDims : null;
}

function collectFilterDimensions(
  filter: QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter,
  dimensions: readonly Dimension[],
  tableName: string,
  newDims: Dimension[]
): void {
  if ('member' in filter) {
    const added = ensureFilterColumnInSchema(
      filter as QueryFilterWithValues,
      [...dimensions, ...newDims],
      tableName
    );
    if (added) newDims.push(...added);
  } else if ('or' in filter) {
    for (const child of (filter as LogicalOrFilter).or) {
      collectFilterDimensions(child as QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter, dimensions, tableName, newDims);
    }
  } else if ('and' in filter) {
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
