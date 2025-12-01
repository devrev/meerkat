import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import { FilterOperator } from '../../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';

const createInOperatorAST = (
  member: string,
  sqlExpression: string
): ParsedExpression => {
  const columnRef: ParsedExpression = {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: member.split(COLUMN_NAME_DELIMITER),
  };

  // Create a placeholder constant node for the SQL expression
  // This will be replaced with actual SQL during query generation
  const sqlExpressionNode: ParsedExpression = {
    class: ExpressionClass.CONSTANT,
    type: ExpressionType.VALUE_CONSTANT,
    alias: '',
    value: { type: { id: 'VARCHAR' }, is_null: false, value: sqlExpression },
  };

  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.COMPARE_IN,
    alias: '',
    children: [columnRef, sqlExpressionNode],
  };
};

const createNotInOperatorAST = (
  member: string,
  sqlExpression: string
): ParsedExpression => {
  const columnRef: ParsedExpression = {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: member.split(COLUMN_NAME_DELIMITER),
  };

  // Create a placeholder constant node for the SQL expression
  // This will be replaced with actual SQL during query generation
  const sqlExpressionNode: ParsedExpression = {
    class: ExpressionClass.CONSTANT,
    type: ExpressionType.VALUE_CONSTANT,
    alias: '',
    value: { type: { id: 'VARCHAR' }, is_null: false, value: sqlExpression },
  };

  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.COMPARE_NOT_IN,
    alias: '',
    children: [columnRef, sqlExpressionNode],
  };
};
/**
 * Creates a proper AST node for SQL expression filters (IN/NOT IN)
 *
 * For simple value lists like "(1, 3)", creates VALUE_CONSTANT nodes.
 * For subqueries like "(SELECT ...)", creates a placeholder that will be replaced during SQL generation.
 *
 * @param member - The member name (e.g., "users.id")
 * @param sqlExpression - The SQL expression fragment (e.g., "(1, 3)" or "(SELECT id FROM ...)")
 * @param operator - The filter operator ("in" or "notIn")
 * @returns ParsedExpression with proper AST structure
 * @throws Error if sqlExpression is missing or invalid
 */
export const getSQLExpressionAST = (
  member: string,
  sqlExpression: string,
  operator: FilterOperator
): ParsedExpression => {
  switch (operator) {
    case 'in':
      return createInOperatorAST(member, sqlExpression);
    case 'notIn':
      return createNotInOperatorAST(member, sqlExpression);
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
};
