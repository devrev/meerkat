import {
  BetweenExpression,
  CaseExpression,
  CastExpression,
  CollateExpression,
  ColumnRefExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ConstantExpression,
  FunctionExpression,
  LambdaExpression,
  OperatorExpression,
  ParameterExpression,
  PositionalReferenceExpression,
  SubqueryExpression,
  WindowExpression,
} from './duckdb-serialization-types/serialization/ParsedExpression';

import {
  ExpressionClass,
  QueryNode,
  QueryNodeType,
  SelectNode,
} from './duckdb-serialization-types';
import { ParsedExpression } from './duckdb-serialization-types/serialization/ParsedExpression';

export const isSelectNode = (node: QueryNode): node is SelectNode => {
  return node.type === QueryNodeType.SELECT_NODE;
};

export const isBetweenExpression = (
  node: ParsedExpression
): node is BetweenExpression => {
  return node.class === ExpressionClass.BETWEEN;
};

export const isCaseExpression = (
  node: ParsedExpression
): node is CaseExpression => {
  return node.class === ExpressionClass.CASE;
};

export const isCastExpression = (
  node: ParsedExpression
): node is CastExpression => {
  return node.class === ExpressionClass.CAST;
};

export const isComparisonExpression = (
  node: ParsedExpression
): node is ComparisonExpression => {
  return node.class === ExpressionClass.COMPARISON;
};

export const isConjunctionExpression = (
  node: ParsedExpression
): node is ConjunctionExpression => {
  return node.class === ExpressionClass.CONJUNCTION;
};

export const isConstantExpression = (
  node: ParsedExpression
): node is ConstantExpression => {
  return node.class === ExpressionClass.CONSTANT;
};

export const isColumnRefExpression = (
  node: ParsedExpression
): node is ColumnRefExpression => {
  return node.class === ExpressionClass.COLUMN_REF;
};

export const isCollateExpression = (
  node: ParsedExpression
): node is CollateExpression => {
  return node.class === ExpressionClass.COLLATE;
};

export const isFunctionExpression = (
  node: ParsedExpression
): node is FunctionExpression => {
  return node.class === ExpressionClass.FUNCTION;
};

export const isLambdaExpression = (
  node: ParsedExpression
): node is LambdaExpression => {
  return node.class === ExpressionClass.LAMBDA;
};

export const isOperatorExpression = (
  node: ParsedExpression
): node is OperatorExpression => {
  return node.class === ExpressionClass.OPERATOR;
};

export const isParameterExpression = (
  node: ParsedExpression
): node is ParameterExpression => {
  return node.class === ExpressionClass.PARAMETER;
};

export const isPositionalReferenceExpression = (
  node: ParsedExpression
): node is PositionalReferenceExpression => {
  return node.class === ExpressionClass.POSITIONAL_REFERENCE;
};

export const isSubqueryExpression = (
  node: ParsedExpression
): node is SubqueryExpression => {
  return node.class === ExpressionClass.SUBQUERY;
};

export const isWindowExpression = (
  node: ParsedExpression
): node is WindowExpression => {
  return node.class === ExpressionClass.WINDOW;
};
