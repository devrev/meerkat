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
