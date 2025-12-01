import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';

/**
 * Creates a placeholder AST node for a SQL expression
 * The actual SQL will be injected later during SQL generation
 *
 * Format: __MEERKAT_SQL_EXPR__{base64EncodedSQL}__
 *
 * @param member - The member name (e.g., "users.id")
 * @param sqlExpression - The SQL expression fragment (e.g., "IN (1, 3)" or just "(1, 3)" for IN operator)
 * @param operator - The filter operator (e.g., "in", "gt", "equals")
 * @returns ParsedExpression with a placeholder that will be replaced later
 */
export const getSQLExpressionAST = (
  member: string,
  sqlExpression: string,
  operator: string
): ParsedExpression => {
  // Convert member name to column reference (e.g., "users.id" -> "users__id")
  const columnRef = member.split('.').join('__');

  // Build the complete SQL expression based on operator
  const fullExpression = buildFullSQLExpression(
    columnRef,
    sqlExpression,
    operator
  );

  // Encode the SQL expression in base64 to avoid SQL injection issues
  const encoded = Buffer.from(fullExpression).toString('base64');
  const placeholder = `__MEERKAT_SQL_EXPR__${encoded}__`;

  // Return a simple column reference that will be replaced during SQL generation
  return {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: [placeholder],
  };
};

/**
 * Build the full SQL expression by combining column reference with the SQL fragment
 *
 * @param columnRef - The aliased column name (e.g., "users__id")
 * @param sqlExpression - The SQL expression fragment from user
 * @param operator - The filter operator
 * @returns Complete SQL expression
 */
function buildFullSQLExpression(
  columnRef: string,
  sqlExpression: string,
  operator: string
): string {
  // Check if the SQL expression already contains the column reference
  // (for backwards compatibility with full expressions)
  if (sqlExpression.includes(columnRef)) {
    return sqlExpression;
  }

  // Build expression based on operator
  switch (operator) {
    case 'in':
    case 'notIn':
      // Handle both "IN (1, 3)" and just "(1, 3)"
      if (
        sqlExpression.trim().toUpperCase().startsWith('IN ') ||
        sqlExpression.trim().toUpperCase().startsWith('NOT IN ')
      ) {
        return `${columnRef} ${sqlExpression}`;
      }
      return operator === 'in'
        ? `${columnRef} IN ${sqlExpression}`
        : `${columnRef} NOT IN ${sqlExpression}`;

    case 'equals':
      return sqlExpression.includes('=')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} = ${sqlExpression}`;

    case 'notEquals':
      return sqlExpression.includes('!=') || sqlExpression.includes('<>')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} != ${sqlExpression}`;

    case 'gt':
      return sqlExpression.includes('>')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} > ${sqlExpression}`;

    case 'gte':
      return sqlExpression.includes('>=')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} >= ${sqlExpression}`;

    case 'lt':
      return sqlExpression.includes('<')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} < ${sqlExpression}`;

    case 'lte':
      return sqlExpression.includes('<=')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} <= ${sqlExpression}`;

    case 'contains':
    case 'notContains':
      // LIKE expressions
      return sqlExpression.toUpperCase().includes('LIKE')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} LIKE ${sqlExpression}`;

    case 'inDateRange':
      // BETWEEN expression
      return sqlExpression.toUpperCase().includes('BETWEEN')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} BETWEEN ${sqlExpression}`;

    case 'notInDateRange':
      // NOT BETWEEN expression
      return sqlExpression.toUpperCase().includes('NOT BETWEEN')
        ? `${columnRef} ${sqlExpression}`
        : `${columnRef} NOT BETWEEN ${sqlExpression}`;

    default:
      // For unknown operators or custom expressions, assume it's a complete expression
      // or just append it to the column reference
      return sqlExpression.includes('=') ||
        sqlExpression.includes('>') ||
        sqlExpression.includes('<') ||
        sqlExpression.toUpperCase().includes('IN') ||
        sqlExpression.toUpperCase().includes('LIKE')
        ? sqlExpression // Already complete
        : `${columnRef} ${sqlExpression}`; // Append to column
  }
}

/**
 * Extract SQL expression placeholders from generated SQL and replace them
 *
 * @param sql - Generated SQL string
 * @returns SQL with placeholders replaced by actual expressions
 */
export const applySQLExpressions = (sql: string): string => {
  // Match pattern: "__MEERKAT_SQL_EXPR__{base64}__" or __MEERKAT_SQL_EXPR__{base64}__
  // The placeholder might be quoted as an identifier by DuckDB
  const quotedPattern = /"__MEERKAT_SQL_EXPR__([A-Za-z0-9+/=]+)__"/g;
  const unquotedPattern = /__MEERKAT_SQL_EXPR__([A-Za-z0-9+/=]+)__/g;

  // First handle quoted placeholders
  let result = sql.replace(quotedPattern, (match, encoded) => {
    // Decode the base64 SQL expression
    const sqlExpression = Buffer.from(encoded, 'base64').toString('utf-8');
    return sqlExpression;
  });

  // Then handle any remaining unquoted placeholders
  result = result.replace(unquotedPattern, (match, encoded) => {
    // Decode the base64 SQL expression
    const sqlExpression = Buffer.from(encoded, 'base64').toString('utf-8');
    return sqlExpression;
  });

  return result;
};
