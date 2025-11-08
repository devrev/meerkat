import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import { Dimension, Measure } from '../../types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

const inDuckDbCondition = (
  columnName: string,
  values: string[],
  memberInfo: Measure | Dimension
) => {
  const sqlTreeValues = values.map((value) => {
    return {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: valueBuilder(value, memberInfo),
    };
  });
  switch (memberInfo.type) {
    case 'number_array':
    case 'string_array': {
      return {
        class: ExpressionClass.FUNCTION,
        type: ExpressionType.FUNCTION,
        alias: '',
        function_name: '&&',
        schema: '',
        children: [
          {
            class: 'COLUMN_REF',
            type: 'COLUMN_REF',
            alias: '',
            column_names: columnName.split(COLUMN_NAME_DELIMITER),
          },
          {
            class: ExpressionClass.OPERATOR,
            type: ExpressionType.ARRAY_CONSTRUCTOR,
            alias: '',
            children: sqlTreeValues,
          },
        ],
        filter: null,
        order_bys: {
          type: 'ORDER_MODIFIER',
          orders: [],
        },
        distinct: false,
        is_operator: true,
        export_state: false,
        catalog: '',
      };
    }
    default: {
      return {
        class: ExpressionClass.SUBQUERY,
        type: ExpressionType.SUBQUERY,
        alias: '',
        subquery_type: 'ANY',
        subquery: {
          node: {
            type: 'SELECT_NODE',
            modifiers: [],
            cte_map: {
              map: [],
            },
            select_list: [
              {
                class: 'FUNCTION',
                type: 'FUNCTION',
                alias: '',
                function_name: 'unnest',
                schema: '',
                children: [
                  {
                    class: 'CAST',
                    type: 'OPERATOR_CAST',
                    alias: '',
                    child: {
                      class: 'OPERATOR',
                      type: 'ARRAY_CONSTRUCTOR',
                      alias: '',
                      children: sqlTreeValues,
                    },
                    cast_type: {
                      id: 'LIST',
                      type_info: {
                        type: 'LIST_TYPE_INFO',
                        alias: '',
                        modifiers: [],
                        child_type: {
                          id: 'VARCHAR',
                          type_info: null,
                        },
                      },
                    },
                    try_cast: false,
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
          class: 'COLUMN_REF',
          type: 'COLUMN_REF',
          alias: '',
          column_names: ['type'],
        },
        comparison_type: 'COMPARE_EQUAL',
      };
    }
  }
};

export const inTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;
  if (!values) {
    throw new Error('In filter must have at least one value');
  }
  return inDuckDbCondition(member, values, memberInfo);
};
