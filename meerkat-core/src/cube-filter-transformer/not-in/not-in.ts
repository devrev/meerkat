import { Dimension, Measure } from '../../types/cube-types/table';
import { CubeToParseExpressionTransform } from '../factory';

import {
  COLUMN_NAME_DELIMITER,
  STRING_ARRAY_DELIMITER,
} from '../../member-formatters/constants';
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
    case 'string':
    case 'number': {
      /**
       * Doing the string split optimization here because as the number of nodes in the AST increase,
       * the time take to parse the AST increases, thereby increasing the time to generate the SQL.
       */
      const joinedValues = values.join(STRING_ARRAY_DELIMITER);

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
                  // For numeric types, we need to CAST the string result to the appropriate type
                  memberInfo.type === 'number'
                    ? {
                        class: ExpressionClass.CAST,
                        type: ExpressionType.OPERATOR_CAST,
                        alias: '',
                        child: {
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
                        cast_type: {
                          id: 'DOUBLE',
                          type_info: null,
                        },
                        try_cast: false,
                      }
                    : {
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
    default: {
      // For other types, use the standard COMPARE_NOT_IN approach
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
        type: ExpressionType.COMPARE_NOT_IN,
        alias: '',
        children: [columnRef, ...sqlTreeValues],
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
