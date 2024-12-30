import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isCaseExpression,
  isCastExpression,
  isColumnRefExpression,
  isConstantExpression,
  isFunctionExpression,
  isOperatorExpression,
} from '../types/utils';
import { ParsedSerialization } from './types';
import { getSelectNode } from './utils';

/**
 * Validates an individual expression node
 */
export const validateExpressionNode = (
  node: ParsedExpression,
  validFunctions: Set<string>
): boolean => {
  // Column references and value constants
  if (isColumnRefExpression(node) || isConstantExpression(node)) {
    return true;
  }

  // Cast expression
  if (isCastExpression(node)) {
    return validateExpressionNode(node.child, validFunctions);
  }

  // Operator expression
  if (isOperatorExpression(node)) {
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
  const node = getSelectNode(parsedSerialization);

  const validFunctionSet = new Set(validFunctions);

  // Validate the expression
  if (validateExpressionNode(node, validFunctionSet)) {
    return true;
  }

  throw new Error('Expression contains invalid functions or operators');
};
