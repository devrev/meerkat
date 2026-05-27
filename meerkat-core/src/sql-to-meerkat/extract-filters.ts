import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilterWithValues,
} from '../types/cube-types/query';
import { Dimension, Measure } from '../types/cube-types/table';
import {
  BetweenExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../types/duckdb-serialization-types';
import { getColumnName, getConstantValue, matchMeasureFromExpr } from './helpers';

export interface FilterExtractionResult {
  filters: (QueryFilterWithValues | LogicalOrFilter)[];
  residual: ParsedExpression | undefined;
  warnings: string[];
}

export function extractFiltersFromAst(
  whereExpr: ParsedExpression,
  tableName: string
): FilterExtractionResult {
  const filters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const residualParts: ParsedExpression[] = [];
  const warnings: string[] = [];

  if (whereExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = whereExpr as ConjunctionExpression;
    if (whereExpr.type === ExpressionType.CONJUNCTION_AND) {
      for (const child of conj.children) {
        const extracted = tryExtractFilters(child, tableName);
        if (extracted) {
          filters.push(...extracted);
        } else {
          residualParts.push(child);
          warnings.push('Non-extractable WHERE condition retained in base SQL');
        }
      }
    } else if (whereExpr.type === ExpressionType.CONJUNCTION_OR) {
      const orFilter = extractOrFilter(conj, tableName);
      if (orFilter) {
        filters.push(orFilter);
      } else {
        residualParts.push(whereExpr);
        warnings.push(
          'OR condition partially non-extractable, retained in base SQL'
        );
      }
    }
  } else {
    const extracted = tryExtractFilters(whereExpr, tableName);
    if (extracted) {
      filters.push(...extracted);
    } else {
      residualParts.push(whereExpr);
      warnings.push('Non-extractable WHERE condition retained in base SQL');
    }
  }

  const residual = buildResidualConjunction(residualParts);
  return { filters, residual, warnings };
}

export function extractHavingFromAst(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues[] {
  const filters: QueryFilterWithValues[] = [];

  if (havingExpr.class === ExpressionClass.COMPARISON) {
    const comp = havingExpr as ComparisonExpression;
    const measure = matchMeasureFromExpr(comp.left, measures);
    if (measure) {
      const value = getConstantValue(comp.right);
      const opMap: Record<string, QueryFilterWithValues['operator']> = {
        [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
        [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
        [ExpressionType.COMPARE_LESSTHAN]: 'lt',
        [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
        [ExpressionType.COMPARE_EQUAL]: 'equals',
        [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
      };
      const op = opMap[comp.type];
      if (op && value !== null) {
        filters.push({
          member: `${tableName}.${measure.name}`,
          operator: op,
          values: [String(value)],
        });
      }
    }
  } else if (havingExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = havingExpr as ConjunctionExpression;
    for (const child of conj.children) {
      filters.push(...extractHavingFromAst(child, tableName, measures));
    }
  }

  return filters;
}

export function ensureFilterColumnInSchema(
  filter: QueryFilterWithValues,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const colName = filter.member.replace(`${tableName}.`, '');
  const exists = dimensions.some(
    (d) => d.name === colName || d.sql === colName
  );
  if (!exists) {
    return [{ name: colName, sql: colName, type: 'string' }];
  }
  return null;
}

export function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const newDims: Dimension[] = [];
  for (const child of orFilter.or) {
    if ('member' in child) {
      const added = ensureFilterColumnInSchema(
        child as QueryFilterWithValues,
        [...dimensions, ...newDims],
        tableName
      );
      if (added) newDims.push(...added);
    } else if ('and' in child) {
      for (const grandchild of (child as LogicalAndFilter).and) {
        if ('member' in grandchild) {
          const added = ensureFilterColumnInSchema(
            grandchild as QueryFilterWithValues,
            [...dimensions, ...newDims],
            tableName
          );
          if (added) newDims.push(...added);
        }
      }
    }
  }
  return newDims.length > 0 ? newDims : null;
}

function buildResidualConjunction(
  parts: ParsedExpression[]
): ParsedExpression | undefined {
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0];
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_AND,
    alias: '',
    children: parts,
  } as ConjunctionExpression;
}

function extractOrFilter(
  conj: ConjunctionExpression,
  tableName: string
): LogicalOrFilter | null {
  const orChildren: (QueryFilterWithValues | LogicalAndFilter)[] = [];
  for (const child of conj.children) {
    if (
      child.class === ExpressionClass.CONJUNCTION &&
      child.type === ExpressionType.CONJUNCTION_AND
    ) {
      const andChildren: QueryFilterWithValues[] = [];
      const andConj = child as ConjunctionExpression;
      let allExtracted = true;
      for (const grandchild of andConj.children) {
        const extracted = tryExtractFilters(grandchild, tableName);
        if (extracted) {
          andChildren.push(...extracted);
        } else {
          allExtracted = false;
          break;
        }
      }
      if (!allExtracted) return null;
      orChildren.push({ and: andChildren });
    } else {
      const extracted = tryExtractFilters(child, tableName);
      if (!extracted) return null;
      if (extracted.length === 1) {
        orChildren.push(extracted[0]);
      } else {
        orChildren.push({ and: extracted });
      }
    }
  }
  return { or: orChildren };
}

function tryExtractFilters(
  expr: ParsedExpression,
  tableName: string
): QueryFilterWithValues[] | null {
  if (expr.class === ExpressionClass.COMPARISON) {
    const f = extractComparisonFilter(expr as ComparisonExpression, tableName);
    return f ? [f] : null;
  }
  if (expr.class === ExpressionClass.BETWEEN) {
    return extractBetweenFilter(expr as BetweenExpression, tableName);
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    const f = extractOperatorFilter(expr as OperatorExpression, tableName);
    return f ? [f] : null;
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    const f = extractFunctionFilter(expr as FunctionExpression, tableName);
    return f ? [f] : null;
  }
  return null;
}

function extractComparisonFilter(
  expr: ComparisonExpression,
  tableName: string
): QueryFilterWithValues | null {
  const colName = getColumnName(expr.left);
  if (!colName) return null;

  const value = getConstantValue(expr.right);
  if (value === null && expr.right.class !== ExpressionClass.CONSTANT)
    return null;

  // col = NULL / col != NULL → notSet / set (SQL NULL comparison is always false)
  if (value === null && expr.right.class === ExpressionClass.CONSTANT) {
    if (expr.type === ExpressionType.COMPARE_EQUAL) {
      return {
        member: `${tableName}.${colName}`,
        operator: 'notSet',
        values: [],
      };
    }
    if (expr.type === ExpressionType.COMPARE_NOTEQUAL) {
      return {
        member: `${tableName}.${colName}`,
        operator: 'set',
        values: [],
      };
    }
    return null;
  }

  const opMap: Record<string, QueryFilterWithValues['operator']> = {
    [ExpressionType.COMPARE_EQUAL]: 'equals',
    [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
    [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
    [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
    [ExpressionType.COMPARE_LESSTHAN]: 'lt',
    [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
  };

  const operator = opMap[expr.type];
  if (!operator) return null;

  return {
    member: `${tableName}.${colName}`,
    operator,
    values: [String(value)],
  };
}

function extractBetweenFilter(
  expr: BetweenExpression,
  tableName: string
): QueryFilterWithValues[] | null {
  const colName = getColumnName(expr.input);
  if (!colName) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  const isNumeric = typeof lower === 'number' && typeof upper === 'number';
  if (isNumeric) {
    const colNameLower = colName.toLowerCase();
    const looksLikeDate =
      colNameLower.endsWith('_at') ||
      colNameLower.endsWith('_date') ||
      colNameLower.includes('time');
    if (!looksLikeDate) {
      return [
        {
          member: `${tableName}.${colName}`,
          operator: 'gte',
          values: [String(lower)],
        },
        {
          member: `${tableName}.${colName}`,
          operator: 'lte',
          values: [String(upper)],
        },
      ];
    }
  }

  return [
    {
      member: `${tableName}.${colName}`,
      operator: 'inDateRange',
      values: [String(lower), String(upper)],
    },
  ];
}

function extractOperatorFilter(
  expr: OperatorExpression,
  tableName: string
): QueryFilterWithValues | null {
  if (
    expr.type === ExpressionType.OPERATOR_IS_NULL &&
    expr.children?.length === 1
  ) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return {
      member: `${tableName}.${colName}`,
      operator: 'notSet',
      values: [],
    };
  }
  if (
    expr.type === ExpressionType.OPERATOR_IS_NOT_NULL &&
    expr.children?.length === 1
  ) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return { member: `${tableName}.${colName}`, operator: 'set', values: [] };
  }

  const inOperatorMap: Record<string, QueryFilterWithValues['operator']> = {
    [ExpressionType.COMPARE_IN]: 'in',
    [ExpressionType.COMPARE_NOT_IN]: 'notIn',
  };
  const inOp = inOperatorMap[expr.type];
  if (inOp && expr.children?.length >= 2) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    const values: string[] = [];
    for (let i = 1; i < expr.children.length; i++) {
      const val = getConstantValue(expr.children[i]);
      if (val === null) return null;
      values.push(String(val));
    }
    return { member: `${tableName}.${colName}`, operator: inOp, values };
  }

  return null;
}

const LIKE_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  '~~': 'contains',
  '~~*': 'contains',
  '!~~': 'notContains',
  '!~~*': 'notContains',
};

function extractFunctionFilter(
  expr: FunctionExpression,
  tableName: string
): QueryFilterWithValues | null {
  const fnName = expr.function_name.toLowerCase();
  const likeOp = LIKE_OPERATOR_MAP[fnName];
  if (!likeOp || expr.children.length < 2) return null;

  const colName = getColumnName(expr.children[0]);
  if (!colName) return null;

  const patternVal = getConstantValue(expr.children[1]);
  if (patternVal === null) return null;

  const pattern = String(patternVal);
  if (!pattern.startsWith('%') || !pattern.endsWith('%') || pattern.length <= 2)
    return null;

  const inner = pattern.slice(1, -1);
  if (inner.includes('%') || inner.includes('_')) return null;

  return {
    member: `${tableName}.${colName}`,
    operator: likeOp,
    values: [inner],
  };
}

