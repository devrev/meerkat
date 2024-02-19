import { Dimension, Measure } from '../../types/cube-types/index';

import {
  ExpressionClass,
  ExpressionType,
  QueryNodeType,
  SubqueryType,
} from '../../types/duckdb-serialization-types/index';
import { CUBE_TYPE_TO_DUCKDB_TYPE } from '../../utils/cube-type-to-duckdb-type';
import { convertFloatToInt, getTypeInfo } from '../../utils/get-type-info';

export const baseDuckdbCondition = (
  columnName: string,
  type: ExpressionType,
  value: string,
  memberInfo: Measure | Dimension
) => {
  return {
    class: ExpressionClass.COMPARISON,
    type: type,
    alias: '',
    left: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: columnName.split('.'),
    },
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
  memberInfo: Measure | Dimension
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
              {
                class: ExpressionClass.COLUMN_REF,
                type: ExpressionType.COLUMN_REF,
                alias: '',
                column_names: columnName.split('.'),
              },
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
  console.info(memberInfo);
  switch (memberInfo.type) {
    case 'string':
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: value,
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
        value: value,
      };
  }
};
