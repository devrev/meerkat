/**
 * Utility functions for DuckDB AST inspection.
 *
 * All functions operate on DuckDB's JSON-serialized AST nodes (from json_serialize_sql).
 * They use expression class/type enums — never string parsing or regex on SQL text.
 *
 * Split into focused modules:
 * - aggregate-detection: aggregate classification (isAggregateExpr, fetchAggregateFunctions)
 * - naming: name generation (generateAggregateName, deduplicateName, inferTypeFromExpr)
 * - ast-utils: AST node inspection (getConstantValue, extractTableName, matchMeasureFromExpr)
 */

export {
  fetchAggregateFunctions,
  isAggregateExpr,
  isNestedAggregateExpr,
  isWindowExpr,
  isStarExpr,
} from './aggregate-detection';

export {
  exprToName,
  generateAggregateName,
  deduplicateName,
  inferTypeFromExpr,
} from './naming';

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
} from './ast-utils';

export type { QualifiedColumnRef } from './ast-utils';
