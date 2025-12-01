import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { ResultModifierType } from '../../types/duckdb-serialization-types/serialization/ResultModifier';

/**
 * Converts a SQL expression string to a DuckDB ParsedExpression AST node
 *
 * @param sqlExpression - The SQL expression string
 * @returns ParsedExpression that can be used in the AST
 */
export const getSQLExpressionAST = (
  sqlExpression: string
): ParsedExpression => {
  // Return as a RAW SQL expression
  // DuckDB will parse this SQL string directly when deserializing
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: '__meerkat_raw_sql__',
    schema: '',
    children: [
      {
        class: ExpressionClass.CONSTANT,
        type: ExpressionType.VALUE_CONSTANT,
        alias: '',
        value: {
          type: {
            id: 'VARCHAR',
            type_info: null,
          },
          is_null: false,
          value: sqlExpression,
        },
      },
    ],
    filter: null,
    order_bys: {
      type: ResultModifierType.ORDER_MODIFIER,
      orders: [],
    },
    distinct: false,
    is_operator: false,
    export_state: false,
    catalog: '',
  };
};
