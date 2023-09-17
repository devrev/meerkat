import { Value } from './Misc';
import { BoundCaseCheck, BoundParameterData, ColumnBinding, LogicalType } from './Nodes';

export enum ExpressionType {
  INVALID = 'INVALID',
  OPERATOR_CAST = 'OPERATOR_CAST',
  OPERATOR_NOT = 'OPERATOR_NOT',
  OPERATOR_IS_NULL = 'OPERATOR_IS_NULL',
  OPERATOR_IS_NOT_NULL = 'OPERATOR_IS_NOT_NULL',
  COMPARE_EQUAL = 'COMPARE_EQUAL',
  COMPARE_BOUNDARY_START = 'COMPARE_BOUNDARY_START',
  COMPARE_NOTEQUAL = 'COMPARE_NOTEQUAL',
  COMPARE_LESSTHAN = 'COMPARE_LESSTHAN',
  COMPARE_GREATERTHAN = 'COMPARE_GREATERTHAN',
  COMPARE_LESSTHANOREQUALTO = 'COMPARE_LESSTHANOREQUALTO',
  COMPARE_GREATERTHANOREQUALTO = 'COMPARE_GREATERTHANOREQUALTO',
  COMPARE_IN = 'COMPARE_IN',
  COMPARE_NOT_IN = 'COMPARE_NOT_IN',
  COMPARE_DISTINCT_FROM = 'COMPARE_DISTINCT_FROM',
  COMPARE_BETWEEN = 'COMPARE_BETWEEN',
  COMPARE_NOT_BETWEEN = 'COMPARE_NOT_BETWEEN',
  COMPARE_NOT_DISTINCT_FROM = 'COMPARE_NOT_DISTINCT_FROM',
  COMPARE_BOUNDARY_END = 'COMPARE_BOUNDARY_END',
  CONJUNCTION_AND = 'CONJUNCTION_AND',
  CONJUNCTION_OR = 'CONJUNCTION_OR',
  VALUE_CONSTANT = 'VALUE_CONSTANT',
  VALUE_PARAMETER = 'VALUE_PARAMETER',
  VALUE_TUPLE = 'VALUE_TUPLE',
  VALUE_TUPLE_ADDRESS = 'VALUE_TUPLE_ADDRESS',
  VALUE_NULL = 'VALUE_NULL',
  VALUE_VECTOR = 'VALUE_VECTOR',
  VALUE_SCALAR = 'VALUE_SCALAR',
  VALUE_DEFAULT = 'VALUE_DEFAULT',
  AGGREGATE = 'AGGREGATE',
  BOUND_AGGREGATE = 'BOUND_AGGREGATE',
  GROUPING_FUNCTION = 'GROUPING_FUNCTION',
  WINDOW_AGGREGATE = 'WINDOW_AGGREGATE',
  WINDOW_RANK = 'WINDOW_RANK',
  WINDOW_RANK_DENSE = 'WINDOW_RANK_DENSE',
  WINDOW_NTILE = 'WINDOW_NTILE',
  WINDOW_PERCENT_RANK = 'WINDOW_PERCENT_RANK',
  WINDOW_CUME_DIST = 'WINDOW_CUME_DIST',
  WINDOW_ROW_NUMBER = 'WINDOW_ROW_NUMBER',
  WINDOW_FIRST_VALUE = 'WINDOW_FIRST_VALUE',
  WINDOW_LAST_VALUE = 'WINDOW_LAST_VALUE',
  WINDOW_LEAD = 'WINDOW_LEAD',
  WINDOW_LAG = 'WINDOW_LAG',
  WINDOW_NTH_VALUE = 'WINDOW_NTH_VALUE',
  FUNCTION = 'FUNCTION',
  BOUND_FUNCTION = 'BOUND_FUNCTION',
  CASE_EXPR = 'CASE_EXPR',
  OPERATOR_NULLIF = 'OPERATOR_NULLIF',
  OPERATOR_COALESCE = 'OPERATOR_COALESCE',
  ARRAY_EXTRACT = 'ARRAY_EXTRACT',
  ARRAY_SLICE = 'ARRAY_SLICE',
  STRUCT_EXTRACT = 'STRUCT_EXTRACT',
  ARRAY_CONSTRUCTOR = 'ARRAY_CONSTRUCTOR',
  ARROW = 'ARROW',
  SUBQUERY = 'SUBQUERY',
  STAR = 'STAR',
  TABLE_STAR = 'TABLE_STAR',
  PLACEHOLDER = 'PLACEHOLDER',
  COLUMN_REF = 'COLUMN_REF',
  FUNCTION_REF = 'FUNCTION_REF',
  TABLE_REF = 'TABLE_REF',
  CAST = 'CAST',
  BOUND_REF = 'BOUND_REF',
  BOUND_COLUMN_REF = 'BOUND_COLUMN_REF',
  BOUND_UNNEST = 'BOUND_UNNEST',
  COLLATE = 'COLLATE',
  LAMBDA = 'LAMBDA',
  POSITIONAL_REFERENCE = 'POSITIONAL_REFERENCE',
  BOUND_LAMBDA_REF = 'BOUND_LAMBDA_REF',
}

export enum ExpressionClass {
  INVALID = 'INVALID',
  AGGREGATE = 'AGGREGATE',
  CASE = 'CASE',
  CAST = 'CAST',
  COLUMN_REF = 'COLUMN_REF',
  COMPARISON = 'COMPARISON',
  CONJUNCTION = 'CONJUNCTION',
  CONSTANT = 'CONSTANT',
  DEFAULT = 'DEFAULT',
  FUNCTION = 'FUNCTION',
  OPERATOR = 'OPERATOR',
  STAR = 'STAR',
  SUBQUERY = 'SUBQUERY',
  WINDOW = 'WINDOW',
  PARAMETER = 'PARAMETER',
  COLLATE = 'COLLATE',
  LAMBDA = 'LAMBDA',
  POSITIONAL_REFERENCE = 'POSITIONAL_REFERENCE',
  BETWEEN = 'BETWEEN',
  BOUND_AGGREGATE = 'BOUND_AGGREGATE',
  BOUND_CASE = 'BOUND_CASE',
  BOUND_CAST = 'BOUND_CAST',
  BOUND_COLUMN_REF = 'BOUND_COLUMN_REF',
  BOUND_COMPARISON = 'BOUND_COMPARISON',
  BOUND_CONJUNCTION = 'BOUND_CONJUNCTION',
  BOUND_CONSTANT = 'BOUND_CONSTANT',
  BOUND_DEFAULT = 'BOUND_DEFAULT',
  BOUND_FUNCTION = 'BOUND_FUNCTION',
  BOUND_OPERATOR = 'BOUND_OPERATOR',
  BOUND_PARAMETER = 'BOUND_PARAMETER',
  BOUND_REF = 'BOUND_REF',
  BOUND_SUBQUERY = 'BOUND_SUBQUERY',
  BOUND_WINDOW = 'BOUND_WINDOW',
  BOUND_BETWEEN = 'BOUND_BETWEEN',
  BOUND_UNNEST = 'BOUND_UNNEST',
  BOUND_LAMBDA = 'BOUND_LAMBDA',
  BOUND_LAMBDA_REF = 'BOUND_LAMBDA_REF',
  BOUND_EXPRESSION = 'BOUND_EXPRESSION',
}

export interface BaseExpression {
  expression_class: ExpressionClass;
  type: ExpressionType;
  alias: string;
}

export type Expression =
  | BoundBetweenExpression
  | BoundCaseExpression
  | BoundCastExpression
  | BoundColumnRefExpression
  | BoundComparisonExpression
  | BoundConjunctionExpression
  | BoundConstantExpression
  | BoundDefaultExpression
  | BoundLambdaExpression
  | BoundLambdaRefExpression
  | BoundOperatorExpression
  | BoundParameterExpression
  | BoundReferenceExpression
  | BoundUnnestExpression
  | BoundFunctionExpression
  | BoundAggregateExpression
  | BoundWindowExpression;

export interface BoundBetweenExpression extends BaseExpression {
  input: Expression;
  lower: Expression;
  upper: Expression;
  lower_inclusive: boolean;
  upper_inclusive: boolean;
}

export interface BoundCaseExpression extends BaseExpression {
  return_type: LogicalType;
  case_checks: BoundCaseCheck[];
  else_expr: Expression;
}

export interface BoundCastExpression extends BaseExpression {
  child: Expression;
  return_type: LogicalType;
  try_cast: boolean;
}

export interface BoundColumnRefExpression extends BaseExpression {
  return_type: LogicalType;
  binding: ColumnBinding;
  depth: number;
}

export interface BoundComparisonExpression extends BaseExpression {
  left: Expression;
  right: Expression;
}

export interface BoundConjunctionExpression extends BaseExpression {
  children: Expression[];
}

export interface BoundConstantExpression extends BaseExpression {
  value: Value;
}

export interface BoundDefaultExpression extends BaseExpression {
  return_type: LogicalType;
}

export interface BoundLambdaExpression extends BaseExpression {
  return_type: LogicalType;
  lambda_expr: Expression;
  captures: Expression[];
  parameter_count: number;
}

export interface BoundLambdaRefExpression extends BaseExpression {
  return_type: LogicalType;
  binding: ColumnBinding;
  lambda_index: number;
  depth: number;
}

export interface BoundOperatorExpression extends BaseExpression {
  return_type: LogicalType;
  children: Expression[];
}

export interface BoundParameterExpression extends BaseExpression {
  identifier: string;
  return_type: LogicalType;
  parameter_data: BoundParameterData;
}

export interface BoundReferenceExpression extends BaseExpression {
  return_type: LogicalType;
  index: number;
}

export interface BoundUnnestExpression extends BaseExpression {
  return_type: LogicalType;
  child: Expression;
}

export interface BoundFunctionExpression extends BaseExpression {
  // Custom implementation
}

export interface BoundAggregateExpression extends BaseExpression {
  // Custom implementation
}

export interface BoundWindowExpression extends BaseExpression {
  // Custom implementation
}
