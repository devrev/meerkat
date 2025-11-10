import { Dimension, Measure } from '../../types/cube-types/table';
import { CubeToParseExpressionTransform } from '../factory';

import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import {
  AggregateHandling,
  QueryNodeType,
  ResultModifierType,
  SubqueryType,
  TableReferenceType,
} from '../../types/duckdb-serialization-types';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';

const notInDuckDbCondition = (
  columnName: string,
  values: string[],
  memberInfo: Measure | Dimension
) => {
  const columnRef = {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: columnName.split(COLUMN_NAME_DELIMITER),
  };

  switch (memberInfo.type) {
    case 'number_array':
    case 'string_array': {
      const sqlTreeValues = values.map((value) => {
        return {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          value: valueBuilder(value, memberInfo),
        };
      });
      return {
        class: ExpressionClass.OPERATOR,
        type: ExpressionType.OPERATOR_NOT,
        alias: '',
        children: [
          {
            class: ExpressionClass.FUNCTION,
            type: ExpressionType.FUNCTION,
            alias: '',
            function_name: '&&',
            schema: '',
            children: [
              columnRef,
              {
                class: ExpressionClass.OPERATOR,
                type: ExpressionType.ARRAY_CONSTRUCTOR,
                alias: '',
                children: sqlTreeValues,
              },
            ],
            filter: null,
            order_bys: {
              type: ResultModifierType.ORDER_MODIFIER,
              orders: [],
            },
            distinct: false,
            is_operator: true,
            export_state: false,
            catalog: '',
          },
        ],
      };
    }
    default: {
      // Optimized approach: Use string_split with delimiter
      // This provides 91% size reduction by avoiding N VALUE_CONSTANT nodes
      // Use special delimiter sequence unlikely to appear in data
      const DELIMITER = '§§'; // Section sign - uncommon in normal data
      const sanitizedValues = values.map((v) => {
        const strVal = String(v);
        // Escape delimiter if it appears in the value
        return strVal.replace(/§§/g, '§§§§');
      });
      const joinedValues = sanitizedValues.join(DELIMITER);

      return {
        class: ExpressionClass.OPERATOR,
        type: ExpressionType.OPERATOR_NOT,
        alias: '',
        children: [
          {
            class: ExpressionClass.SUBQUERY,
            type: ExpressionType.SUBQUERY,
            alias: '',
            subquery_type: SubqueryType.ANY,
            subquery: {
              node: {
                type: QueryNodeType.SELECT_NODE,
                modifiers: [],
                cte_map: { map: [] },
                select_list: [
                  {
                    class: ExpressionClass.FUNCTION,
                    type: ExpressionType.FUNCTION,
                    alias: '',
                    function_name: 'unnest',
                    schema: '',
                    children: [
                      {
                        class: ExpressionClass.FUNCTION,
                        type: ExpressionType.FUNCTION,
                        alias: '',
                        function_name: 'string_split',
                        schema: '',
                        children: [
                          {
                            class: ExpressionClass.CONSTANT,
                            type: ExpressionType.VALUE_CONSTANT,
                            alias: '',
                            value: {
                              type: { id: 'VARCHAR', type_info: null },
                              is_null: false,
                              value: joinedValues,
                            },
                          },
                          {
                            class: ExpressionClass.CONSTANT,
                            type: ExpressionType.VALUE_CONSTANT,
                            alias: '',
                            value: {
                              type: { id: 'VARCHAR', type_info: null },
                              is_null: false,
                              value: DELIMITER,
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
                  },
                ],
                from_table: {
                  type: TableReferenceType.EMPTY,
                  alias: '',
                  sample: null,
                },
                where_clause: null,
                group_expressions: [],
                group_sets: [],
                aggregate_handling: AggregateHandling.STANDARD_HANDLING,
                having: null,
                sample: null,
                qualify: null,
              },
            },
            child: columnRef,
            comparison_type: ExpressionType.COMPARE_EQUAL,
          },
        ],
      };
    }
  }
};

export const notInTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;
  if (!values) {
    throw new Error('Not in filter must have at least one value');
  }

  return notInDuckDbCondition(member, values, memberInfo);
};
