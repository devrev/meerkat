import {
  CaseExpression,
  CastExpression,
  ColumnRefExpression,
  ConstantExpression,
  FunctionExpression,
  OperatorExpression,
  SubqueryExpression,
  WindowExpression,
} from './duckdb-serialization-types/serialization/ParsedExpression';

import { ExpressionType } from './duckdb-serialization-types';
import { ParsedExpression } from './duckdb-serialization-types/serialization/ParsedExpression';

export const isColumnRefExpression = (
  node: ParsedExpression
): node is ColumnRefExpression => {
  return node.type === ExpressionType.COLUMN_REF;
};

export const isValueConstantExpression = (
  node: ParsedExpression
): node is ConstantExpression => {
  return node.type === ExpressionType.VALUE_CONSTANT;
};

export const isOperatorCast = (
  node: ParsedExpression
): node is CastExpression => {
  return node.type === ExpressionType.OPERATOR_CAST;
};

export const isCoalesceExpression = (
  node: ParsedExpression
): node is OperatorExpression => {
  return node.type === ExpressionType.OPERATOR_COALESCE;
};

export const isCaseExpression = (
  node: ParsedExpression
): node is CaseExpression => {
  return node.type === ExpressionType.CASE_EXPR;
};

export const isFunctionExpression = (
  node: ParsedExpression
): node is FunctionExpression => {
  return node.type === ExpressionType.FUNCTION;
};

export const isWindowExpression = (
  node: ParsedExpression
): node is WindowExpression => {
  return (
    node.type === ExpressionType.WINDOW_AGGREGATE ||
    node.type === ExpressionType.WINDOW_LAG
  );
};

export const isSubqueryExpression = (
  node: ParsedExpression
): node is SubqueryExpression => {
  return node.type === ExpressionType.SUBQUERY;
};
