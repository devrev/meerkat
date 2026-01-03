import { Dimension, Measure } from '../../types/cube-types/index';

import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import { sanitizeStringValue } from '../../member-formatters/sanitize-value';
import {
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  QueryNodeType,
  SubqueryType,
} from '../../types/duckdb-serialization-types/index';
import { CUBE_TYPE_TO_DUCKDB_TYPE } from '../../utils/cube-type-to-duckdb-type';
import { convertFloatToInt, getTypeInfo } from '../../utils/get-type-info';

/**
 * Options for creating a column reference.
 */
export interface CreateColumnRefOptions {
  /**
   * When true, the columnName is an alias (e.g., "orders.customer_id" as a single column name).
   * When false, the columnName is a table.column reference to be split.
   */
  isAlias: boolean;
}

/**
 * Creates a column reference AST node.
 *
 * @param columnName - The column name or member key
 * @param options - Configuration options
 * @returns A column reference expression for DuckDB AST
 *
 * @example
 * ```typescript
 * // Base table reference (isAlias: false)
 * createColumnRef("orders.customer_id") // column_names: ["orders", "customer_id"]
 *
 * // Alias reference (isAlias: true, useDotNotation: true)
 * createColumnRef("orders.customer_id", { isAlias: true, useDotNotation: true })
 * // column_names: ["orders.customer_id"]
 *
 * // Alias reference (isAlias: true, useDotNotation: false - legacy)
 * createColumnRef("orders__customer_id", { isAlias: true })
 * // column_names: ["orders__customer_id"]
 * ```
 */
export const createColumnRef = (
  columnName: string,
  options: CreateColumnRefOptions
): ParsedExpression => {
  let columnNames: string[];

  if (options.isAlias) {
    // When it's an alias, don't split - use as single column name
    // This is used for PROJECTION_FILTER where we reference projected aliases
    columnNames = [columnName];
  } else {
    // When it's a base table reference, split by delimiter
    // This is used for BASE_FILTER where we reference table.column
    columnNames = columnName.split(COLUMN_NAME_DELIMITER);
  }

  return {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: columnNames,
  };
};

export const baseDuckdbCondition = (
  columnName: string,
  type: ExpressionType,
  value: string,
  memberInfo: Measure | Dimension,
  options: CreateColumnRefOptions
) => {
  return {
    class: ExpressionClass.COMPARISON,
    type: type,
    alias: '',
    left: createColumnRef(columnName, options),
    right: {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: valueBuilder(value, memberInfo),
    },
  };
};

export const baseArrayDuckdbCondition = (
  columnName: string,
  type: ExpressionType,
  value: string,
  memberInfo: Measure | Dimension,
  options: CreateColumnRefOptions
) => {
  return {
    class: ExpressionClass.SUBQUERY,
    type: ExpressionType.SUBQUERY,
    alias: '',
    subquery_type: SubqueryType.ANY,
    subquery: {
      node: {
        type: QueryNodeType.SELECT_NODE,
        modifiers: [],
        cte_map: {
          map: [],
        },
        select_list: [
          {
            class: ExpressionClass.FUNCTION,
            type: ExpressionType.FUNCTION,
            alias: '',
            function_name: 'unnest',
            schema: '',
            children: [
              createColumnRef(columnName, {
                isAlias: options.isAlias,
              }),
            ],
            filter: null,
            order_bys: {
              type: 'ORDER_MODIFIER',
              orders: [],
            },
            distinct: false,
            is_operator: false,
            export_state: false,
            catalog: '',
          },
        ],
        from_table: {
          type: 'EMPTY',
          alias: '',
          sample: null,
        },
        where_clause: null,
        group_expressions: [],
        group_sets: [],
        aggregate_handling: 'STANDARD_HANDLING',
        having: null,
        sample: null,
        qualify: null,
      },
    },
    child: {
      class: 'CONSTANT',
      type: 'VALUE_CONSTANT',
      alias: '',
      value: valueBuilder(value, memberInfo),
    },
    comparison_type: type,
  };
};

export const valueBuilder = (
  value: string,
  memberInfo: Measure | Dimension
) => {
  switch (memberInfo.type) {
    case 'string':
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: sanitizeStringValue(value),
      };

    case 'number': {
      const parsedValue = parseFloat(value);
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: getTypeInfo(parsedValue),
        },
        is_null: false,
        value: convertFloatToInt(parsedValue),
      };
    }
    case 'boolean': {
      const parsedValue = value === 'true';
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: parsedValue,
      };
    }
    case 'time':
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: value,
      };

    default:
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE.string,
          type_info: null,
        },
        is_null: false,
        value: sanitizeStringValue(value),
      };
  }
};
