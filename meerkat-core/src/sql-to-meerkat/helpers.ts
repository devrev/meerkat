/**
 * Barrel re-export for all helper modules.
 *
 * Modules live under helpers/:
 * - aggregate-detection: aggregate classification
 * - naming: name generation and type inference
 * - ast-utils: AST node inspection (constants, table refs, measure matching)
 * - extract-filters: WHERE/HAVING filter extraction
 * - extract-order: ORDER BY extraction
 * - build-base-sql: base SQL construction
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

export {
  extractFiltersFromAst,
  extractHavingFromAst,
  ensureFilterColumnInSchema,
  ensureOrFilterColumnsInSchema,
} from './helpers/extract-filters';

export type { FilterExtractionResult } from './helpers/extract-filters';

export { extractOrderFromAst } from './helpers/extract-order';

export { buildBaseSQL } from './helpers/build-base-sql';
