import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import { FilterOperator } from '../../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';

const getSQLPlaceholder = (sqlExpression: string): string => {
  return `__MEERKAT_SQL_EXPR__${Buffer.from(sqlExpression).toString(
    'base64'
  )}__`;
};

const createInOperatorAST = (
  member: string,
  sqlExpression: string
): ParsedExpression => {
  const sqlPlaceholder = getSQLPlaceholder(sqlExpression);
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
    value: { type: { id: 'VARCHAR' }, is_null: false, value: sqlPlaceholder },
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
  const sqlPlaceholder = getSQLPlaceholder(sqlExpression);

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
    value: { type: { id: 'VARCHAR' }, is_null: false, value: sqlPlaceholder },
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
      throw new Error(
        `SQL expressions are not supported for ${operator} operator. Only "in" and "notIn" operators support SQL expressions.`
      );
  }
};

/**
 * Extract SQL expression placeholders from generated SQL and replace them
 *
 * @param sql - Generated SQL string
 * @returns SQL with placeholders replaced by actual expressions
 */
export const applySQLExpressions = (sql: string): string => {
  // Replace quoted placeholders (DuckDB uses single quotes for VARCHAR constants)
  return sql.replace(
    /'__MEERKAT_SQL_EXPR__([A-Za-z0-9+/=]+)__'/g,
    (match, encoded) => {
      // Decode the base64 SQL expression
      const sqlExpression = Buffer.from(encoded, 'base64').toString('utf-8');
      return sqlExpression;
    }
  );
};
