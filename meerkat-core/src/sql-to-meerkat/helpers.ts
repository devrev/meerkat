/**
 * Utility functions for DuckDB AST inspection.
 *
 * All functions operate on DuckDB's JSON-serialized AST nodes (from json_serialize_sql).
 * They use expression class/type enums — never string parsing or regex on SQL text.
 *
 * Split into focused modules:
 * - helpers/aggregate-detection: aggregate classification
 * - helpers/naming: name generation and type inference
 * - helpers/ast-utils: AST node inspection (constants, table refs, measure matching)
 */

export {
  fetchAggregateFunctions,
  isAggregateExpr,
  isNestedAggregateExpr,
  isWindowExpr,
  isStarExpr,
} from './helpers/aggregate-detection';

export {
  exprToName,
  generateAggregateName,
  deduplicateName,
  inferTypeFromExpr,
} from './helpers/naming';

export {
  stripQueryLocationInPlace,
  getQualifiedColumnRef,
  getColumnName,
  getConstantTypeId,
  isNullConstant,
  getConstantValue,
  extractTableName,
  hasRecursiveCteInMap,
  sanitizeForSerialize,
  matchMeasureFromExpr,
} from './helpers/ast-utils';

export type { QualifiedColumnRef } from './helpers/ast-utils';
