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

export {
  extractFiltersFromAst,
  extractHavingFromAst,
  ensureFilterColumnInSchema,
  ensureOrFilterColumnsInSchema,
} from './extract-filters';

export type { FilterExtractionResult } from './extract-filters';

export { extractOrderFromAst } from './extract-order';

export { buildBaseSQL } from './build-base-sql';
