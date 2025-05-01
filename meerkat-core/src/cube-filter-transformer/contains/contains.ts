import { Member, QueryFilter } from '../../types/cube-types/query';
import { Dimension, Measure } from '../../types/cube-types/table';

import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export interface ContainsFilters extends QueryFilter {
  member: Member;
  operator: 'contains';
  values: string[];
}

export const containsDuckdbCondition = (
  columnName: string,
  value: string,
  memberInfo: Measure | Dimension
) => {
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: '~~*',
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

export const containsTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;

  if (!values || values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple Contains condition
   */
  if (values.length === 1) {
    return containsDuckdbCondition(member, values[0], memberInfo);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      containsDuckdbCondition(member, value, memberInfo)
    );
  });
  return orCondition;
};
