import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isCaseExpression,
  isCoalesceExpression,
  isColumnRefExpression,
  isFunctionExpression,
  isOperatorCast,
  isSelectNode,
  isSubqueryExpression,
  isValueConstantExpression,
  isWindowExpression,
} from '../types/utils';
import { validateSelectNode } from './common';
import { ParsedSerialization } from './types';

export const validateExpressionNode = ({
  node,
  validFunctions,
  parentNode,
  validScalarFunctions,
  hasAggregation = false,
}: {
  node: ParsedExpression;
  validFunctions: Set<string>;
  parentNode: ParsedExpression | null;
  hasAggregation?: boolean;
  validScalarFunctions: Set<string>;
}): boolean => {
  // Base cases for column references and constants
  if (isColumnRefExpression(node) || isValueConstantExpression(node)) {
    // Allow column references inside aggregation functions
    return !!parentNode;
  }

  // Check for valid aggregation functions
  if (isFunctionExpression(node) || isWindowExpression(node)) {
    // count_star don't have children
    if (node.function_name === 'count_star') return true;

    // This is a valid aggregation function - verify its children don't contain nested aggregations
    if (validFunctions.has(node.function_name)) {
      return node.children.some((child) =>
        validateExpressionNode({
          node: child,
          validFunctions,
          parentNode: node,
          validScalarFunctions,
        })
      );
    }
    // For non-aggregation functions
    if (validScalarFunctions.has(node.function_name)) {
      return node.children.some((child) => {
        return (
          validateExpressionNode({
            node: child,
            validFunctions,
            parentNode: node,
            validScalarFunctions,
          }) &&
          (containsAggregation(child, validFunctions) || hasAggregation)
        );
      });
    }

    throw new Error(`Invalid function type: ${node.function_name}`);
  }

  // Case expression
  if (isCaseExpression(node)) {
    const checksValid = node.case_checks.every((caseCheck) => {
      // WHEN conditions cannot contain aggregations
      const whenValid = !containsAggregation(
        caseCheck.when_expr,
        validFunctions
      );

      // THEN expressions must be valid aggregations or contain no aggregations
      const thenValid =
        validateExpressionNode({
          node: caseCheck.then_expr,
          validFunctions,
          parentNode: node,
          validScalarFunctions,
        }) || !containsAggregation(caseCheck.then_expr, validFunctions);
      return whenValid && thenValid;
    });

    const elseValid =
      validateExpressionNode({
        node: node.else_expr,
        validFunctions,
        parentNode: node,
        validScalarFunctions,
      }) || !containsAggregation(node.else_expr, validFunctions);

    return checksValid && elseValid;
  }

  // Subquery expression
  if (isSubqueryExpression(node) && isSelectNode(node.subquery.node)) {
    return node.subquery.node.select_list.every((node) => {
      return validateExpressionNode({
        node,
        validFunctions,
        parentNode,
        validScalarFunctions,
      });
    });
  }

  // Window expression
  if (isWindowExpression(node)) {
    return node.children.every((node) => {
      return validateExpressionNode({
        node,
        validFunctions,
        parentNode,
        validScalarFunctions,
      });
    });
  }

  // Operator cast expression
  if (isOperatorCast(node)) {
    return validateExpressionNode({
      node: node.child,
      validFunctions,
      parentNode,
      validScalarFunctions,
    });
  }

  // Coalesce expression
  if (isCoalesceExpression(node)) {
    return node.children.every(
      (child) =>
        validateExpressionNode({
          node: child,
          validFunctions,
          parentNode,
          validScalarFunctions,
        }) || !containsAggregation(child, validFunctions)
    );
  }

  throw new Error(`Invalid expression type: ${node.type}`);
};

export const containsAggregation = (
  node: ParsedExpression,
  validFunctions: Set<string>
): boolean => {
  if (!node) return false;

  // Function expression
  if (isFunctionExpression(node) || isWindowExpression(node)) {
    return (
      validFunctions.has(node.function_name) ||
      node.children.some((child) => containsAggregation(child, validFunctions))
    );
  }

  // Case expression
  if (isCaseExpression(node)) {
    return (
      node.case_checks.some(
        (check) =>
          containsAggregation(check.when_expr, validFunctions) ||
          containsAggregation(check.then_expr, validFunctions)
      ) || containsAggregation(node.else_expr, validFunctions)
    );
  }

  // Coalesce expression
  if (isCoalesceExpression(node)) {
    return node.children.some((child) =>
      containsAggregation(child, validFunctions)
    );
  }

  // Operator cast expression
  if (isOperatorCast(node)) {
    return containsAggregation(node.child, validFunctions);
  }

  // Window expression
  if (isWindowExpression(node)) {
    return node.children.some((child) =>
      containsAggregation(child, validFunctions)
    );
  }

  // Subquery expression
  if (isSubqueryExpression(node) && isSelectNode(node.subquery.node)) {
    return node.subquery.node.select_list.every((node) => {
      return containsAggregation(node, validFunctions);
    });
  }

  return false;
};

export const validateMeasure = (
  parsedSerialization: ParsedSerialization,
  validFunctions: string[],
  validScalarFunctions: string[]
): boolean => {
  const node = validateSelectNode(parsedSerialization);

  const validFunctionSet = new Set(validFunctions);
  const validScalarFunctionSet = new Set(validScalarFunctions);

  // Validate the expression
  if (
    validateExpressionNode({
      node: node,
      validFunctions: validFunctionSet,
      parentNode: null,
      validScalarFunctions: validScalarFunctionSet,
    })
  ) {
    return true;
  }

  throw new Error('Expression contains invalid functions or operators');
};
