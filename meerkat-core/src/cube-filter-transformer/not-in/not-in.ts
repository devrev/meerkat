import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { Dimension, Measure } from '../../types/cube-types/table';
import { CubeToParseExpressionTransform } from '../factory';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
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
  const sqlTreeValues = values.map((value) => {
    return {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: valueBuilder(value, memberInfo),
    };
  });
  const columnRef = {
    class: 'COLUMN_REF',
    type: 'COLUMN_REF',
    alias: '',
    column_names: columnName.split(COLUMN_NAME_DELIMITER),
  };
  switch (memberInfo.type) {
    case 'number_array':
    case 'string_array': {
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
              type: 'ORDER_MODIFIER',
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
  const { member, memberInfo } = query;

  // Check if this is a SQL expression
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(query.sql);
  }

  if (!query.values) {
    throw new Error('Not in filter must have at least one value');
  }

  return notInDuckDbCondition(member, query.values, memberInfo);
};
