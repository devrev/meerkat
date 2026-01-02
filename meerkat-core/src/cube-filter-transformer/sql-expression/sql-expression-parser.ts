import { FilterOperator } from '../../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import {
  createColumnRef,
  CreateColumnRefOptions,
} from '../base-condition-builder/base-condition-builder';

/**
 * Encode a string to base64 (works in both browser and Node.js)
 */
const toBase64 = (str: string): string => {
  const bytes = new TextEncoder().encode(str);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
};

/**
 * Decode a base64 string (works in both browser and Node.js)
 */
const fromBase64 = (base64: string): string => {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new TextDecoder().decode(bytes);
};

const getSQLPlaceholder = (sqlExpression: string): string => {
  return `__MEERKAT_SQL_EXPR__${toBase64(sqlExpression)}__`;
};

const createInOperatorAST = (
  member: string,
  sqlExpression: string,
  options: CreateColumnRefOptions
): ParsedExpression => {
  const sqlPlaceholder = getSQLPlaceholder(sqlExpression);
  const columnRef = createColumnRef(member, options);

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
  sqlExpression: string,
  options: CreateColumnRefOptions
): ParsedExpression => {
  const sqlPlaceholder = getSQLPlaceholder(sqlExpression);

  const columnRef = createColumnRef(member, options);

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
  operator: FilterOperator,
  options: CreateColumnRefOptions
): ParsedExpression => {
  switch (operator) {
    case 'in':
      return createInOperatorAST(member, sqlExpression, options);
    case 'notIn':
      return createNotInOperatorAST(member, sqlExpression, options);
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
    (_, encoded) => {
      // Decode the base64 SQL expression
      return fromBase64(encoded);
    }
  );
};
