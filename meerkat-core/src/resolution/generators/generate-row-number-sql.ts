import { memberKeyToSafeKey } from '../../member-formatters';

/**
 * Generates row_number() OVER (ORDER BY ...) SQL based on query order.
 * This is used to preserve the original query ordering through resolution operations.
 *
 * @param query - The query object that may contain an order clause
 * @param dimensions - The dimensions array from the base table schema
 * @param baseTableName - The base table name to use in column references
 * @returns SQL expression for row_number() OVER (ORDER BY ...)
 */
export const generateRowNumberSql = (
  query: { order?: Record<string, string> },
  dimensions: { name: string; alias?: string }[],
  baseTableName: string
): string => {
  let rowNumberSql = 'row_number() OVER (';
  if (query.order && Object.keys(query.order).length > 0) {
    const orderClauses = Object.entries(query.order).map(
      ([member, direction]) => {
        // Find the actual column name/alias in the base table dimensions
        const safeMember = memberKeyToSafeKey(member);
        const dimension = dimensions.find(
          (d) => d.name === safeMember || d.alias === safeMember
        );
        const columnName = dimension
          ? dimension.alias || dimension.name
          : safeMember;
        return `${baseTableName}."${columnName}" ${direction.toUpperCase()}`;
      }
    );
    rowNumberSql += `ORDER BY ${orderClauses.join(', ')}`;
  }
  rowNumberSql += ')';
  return rowNumberSql;
};
