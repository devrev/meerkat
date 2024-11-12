import { Dimension, Measure } from '../../types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

const likeDuckDbCondition = (
  columnName: string,
  values: string,
  memberInfo: Measure | Dimension
) => {
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: '~~',
    schema: '',
    children: [
      {
        class: 'COLUMN_REF',
        type: 'COLUMN_REF',
        alias: '',
        column_names: columnName.split('.'),
      },
      {
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        alias: '',
        value: valueBuilder(values, memberInfo),
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
};

export const likeTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;
  if (memberInfo.type !== 'string') {
    throw new Error('Like filter must be on a string column');
  }
  if (!values) {
    throw new Error('Like filter must have at least one value');
  }
  if (values.length === 1) {
    return likeDuckDbCondition(member, values[0], memberInfo);
  }

  const orCondition = orDuckdbCondition();
  for (const value of values) {
    orCondition.children.push(likeDuckDbCondition(member, value, memberInfo));
  }
  return orCondition;
};
