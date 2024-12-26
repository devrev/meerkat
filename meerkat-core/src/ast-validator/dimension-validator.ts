import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isCaseExpression,
  isCoalesceExpression,
  isColumnRefExpression,
  isFunctionExpression,
  isOperatorCast,
  isValueConstantExpression,
} from '../types/utils';
import { validateSelectNode } from './common';
import { ParsedSerialization } from './types';

/**
 * Validates an individual expression node
 */
export const validateExpressionNode = (
  node: ParsedExpression,
  validFunctions: Set<string>
): boolean => {
  // Column references and value constants
  if (isColumnRefExpression(node) || isValueConstantExpression(node)) {
    return true;
  }

  // Operator cast
  if (isOperatorCast(node)) {
    return validateExpressionNode(node.child, validFunctions);
  }

  // Coalesce expression
  if (isCoalesceExpression(node)) {
    return node.children.every((child) =>
      validateExpressionNode(child, validFunctions)
    );
  }

  // Function expression
  if (isFunctionExpression(node)) {
    if (!validFunctions.has(node.function_name)) {
      throw new Error(`Invalid function: ${node.function_name}`);
    }
    return node.children.every((child) =>
      validateExpressionNode(child, validFunctions)
    );
  }

  // Case expression
  if (isCaseExpression(node)) {
    return (
      node.case_checks.every((check) =>
        validateExpressionNode(check.then_expr, validFunctions)
      ) && validateExpressionNode(node.else_expr, validFunctions)
    );
  }

  throw new Error(`Invalid expression type: ${node.type}`);
};

/**
 * Validates if the parsed serialization represents a valid dimension
 */
export const validateDimension = (
  parsedSerialization: ParsedSerialization,
  validFunctions: string[]
): boolean => {
  const node = validateSelectNode(parsedSerialization);

  const validFunctionSet = new Set(validFunctions);

  // Validate the expression
  if (!validateExpressionNode(node, validFunctionSet)) {
    throw new Error('Expression contains invalid functions or operators');
  }

  return true;
};
