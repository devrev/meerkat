import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isCaseExpression,
  isCoalesceExpression,
  isColumnRefExpression,
  isFunctionExpression,
  isOperatorCast,
  isSubqueryExpression,
  isValueConstantExpression,
  isWindowAggregateExpression,
  isWindowLagExpression,
} from '../types/utils';
import { ParsedSerialization } from './types';

const validAggregations = [
  'function_name',
  'entropy',
  'avg',
  'bool_and',
  'quantile_disc',
  'covar_pop',
  'covar_samp',
  'max_by',
  'count',
  'array_agg',
  'favg',
  'quantile_cont',
  'first',
  'bit_and',
  'min_by',
  'string_agg',
  'argmin',
  'mean',
  'histogram',
  'skewness',
  'arg_max',
  'list',
  'variance',
  'mad',
  'regr_syy',
  'mode',
  'bit_or',
  'sumkahan',
  'approx_quantile',
  'kahan_sum',
  'quantile',
  'regr_avgx',
  'regr_avgy',
  'regr_count',
  'regr_intercept',
  'regr_r2',
  'regr_slope',
  'regr_sxx',
  'reservoir_quantile',
  'sem',
  'product',
  'corr',
  'bit_xor',
  'stddev',
  'stddev_pop',
  'stddev_samp',
  'sum',
  'sum_no_overflow',
  'var_pop',
  'kurtosis',
  'arg_min',
  'var_samp',
  'regr_sxy',
  'arbitrary',
  'any_value',
  'last',
  'count_star',
  'bool_or',
  'group_concat',
  'min',
  'bitstring_agg',
  'max',
  'argmax',
  'fsum',
  'median',
  'approx_count_distinct',
];

const validOperators = ['+', '-', '*', '/', '||'];

export const validateExpressionNode = ({
  node,
  validFunctions,
  parentNode,
  hasAggregation = false,
}: {
  node: ParsedExpression;
  validFunctions: Set<string>;
  parentNode: ParsedExpression | null;
  hasAggregation?: boolean;
}): boolean => {
  // Base cases for column references and constants
  if (isColumnRefExpression(node) || isValueConstantExpression(node)) {
    // Allow column references inside aggregation functions
    return !!parentNode;
  }

  // Check for valid aggregation functions
  if (isFunctionExpression(node) || isWindowAggregateExpression(node)) {
    if (node.function_name === 'count_star') {
      return true;
    }

    if (validAggregations.includes(node.function_name)) {
      // This is a valid aggregation function - verify its children don't contain nested aggregations
      return node.children.some((child) =>
        validateExpressionNode({
          node: child,
          validFunctions,
          parentNode: node,
        })
      );
    }
    // For non-aggregation functions
    if (validFunctions.has(node.function_name)) {
      return node.children.some((child) => {
        return (
          validateExpressionNode({
            node: child,
            validFunctions,
            parentNode: node,
          }) &&
          (containsAggregation(child) || hasAggregation)
        );
      });
    }
  }

  // Handle CASE expressions
  if (isCaseExpression(node)) {
    const checksValid = node.case_checks.every((caseCheck) => {
      // WHEN conditions cannot contain aggregations
      const whenValid = !containsAggregation(caseCheck.when_expr);
      // THEN expressions must be valid aggregations or contain no aggregations
      const thenValid =
        validateExpressionNode({
          node: caseCheck.then_expr,
          validFunctions,
          parentNode: node,
        }) || !containsAggregation(caseCheck.then_expr);
      return whenValid && thenValid;
    });

    const elseValid =
      validateExpressionNode({
        node: node.else_expr,
        validFunctions,
        parentNode: node,
      }) || !containsAggregation(node.else_expr);

    return checksValid && elseValid;
  }

  if (isSubqueryExpression(node)) {
    return node.subquery.node.select_list.every((node) => {
      return validateExpressionNode({
        node,
        validFunctions,
        parentNode,
      });
    });
  }

  if (isWindowLagExpression(node)) {
    return node.children.every((node) => {
      return validateExpressionNode({
        node,
        validFunctions,
        parentNode,
      });
    });
  }

  if (isOperatorCast(node)) {
    return validateExpressionNode({
      node: node.child,
      validFunctions,
      parentNode,
    });
  }

  // Handle COALESCE
  if (isCoalesceExpression(node)) {
    return node.children.every(
      (child) =>
        validateExpressionNode({
          node: child,
          validFunctions,
          parentNode,
        }) || !containsAggregation(child)
    );
  }

  return false;
};

const containsAggregation = (node: ParsedExpression): boolean => {
  if (!node) return false;

  if (isFunctionExpression(node) || isWindowAggregateExpression(node)) {
    return (
      validAggregations.includes(node.function_name) ||
      node.children.some(containsAggregation)
    );
  }

  if (isCaseExpression(node)) {
    return (
      node.case_checks.some(
        (check) =>
          containsAggregation(check.when_expr) ||
          containsAggregation(check.then_expr)
      ) || containsAggregation(node.else_expr)
    );
  }

  if (
    isCoalesceExpression(node) ||
    (isFunctionExpression(node) && validOperators.includes(node.function_name))
  ) {
    return node.children.some(containsAggregation);
  }

  if (isOperatorCast(node)) {
    return containsAggregation(node.child);
  }

  if (isWindowLagExpression(node)) {
    return node.children.some(containsAggregation);
  }

  if (isSubqueryExpression(node)) {
    return node.subquery.node.select_list.every((node) => {
      return containsAggregation(node);
    });
  }

  return false;
};

export const validateMeasure = (
  parsedSerialization: ParsedSerialization,
  validFunctions: string[]
): boolean => {
  const statement = parsedSerialization.statements?.[0];
  if (!statement) {
    throw new Error('No statement found');
  }

  if (statement.node.type !== 'SELECT_NODE') {
    throw new Error('Statement must be a SELECT node');
  }

  const selectList = statement.node.select_list;
  if (!selectList?.length || selectList.length !== 1) {
    throw new Error('SELECT must contain exactly one expression');
  }

  const validFunctionSet = new Set(validFunctions);

  // Validate the expression
  const expression = selectList[0];
  if (
    !validateExpressionNode({
      node: expression,
      validFunctions: validFunctionSet,
      parentNode: null,
    })
  ) {
    throw new Error('Expression contains invalid functions or operators');
  }

  return true;
};
