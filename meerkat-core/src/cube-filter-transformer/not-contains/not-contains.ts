import { Member, QueryFilter } from '../../types/cube-types/query';
import { Dimension, Measure } from '../../types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';
import { COLUMN_NAME_DELIMITER } from '../constant';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export interface NotContainsFilters extends QueryFilter {
  member: Member;
  operator: 'notContains';
  values: string[];
}

export const notContainsDuckdbCondition = (
  columnName: string,
  value: string,
  memberInfo: Measure | Dimension
) => {
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: '!~~',
    schema: '',
    children: [
      {
        class: 'COLUMN_REF',
        type: 'COLUMN_REF',
        alias: '',
        column_names: columnName.split(COLUMN_NAME_DELIMITER),
      },
      {
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        alias: '',
        value: valueBuilder(`%${value}%`, memberInfo),
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

export const notContainsTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;

  if (!values || values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple Contains condition
   */
  if (values.length === 1) {
    return notContainsDuckdbCondition(member, values[0], memberInfo);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      notContainsDuckdbCondition(member, value, memberInfo)
    );
  });
  return orCondition;
};
