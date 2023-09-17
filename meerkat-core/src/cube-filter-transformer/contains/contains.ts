import { Member, QueryFilter } from '@devrev/cube-types';
import { ParsedExpression } from '@devrev/duckdb-serialization-types';
import {
  ExpressionClass,
  ExpressionType,
} from 'duckdb-serialization-types/src/serialization/Expression';
import { orDuckdbCondition } from '../or/or';

export interface ContainsFilters extends QueryFilter {
  member: Member;
  operator: 'contains';
  values: string[];
}

export const containsDuckdbCondition = (columnName: string, value: string) => {
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
        column_names: [columnName],
      },
      {
        class: 'COLUMN_REF',
        type: 'COLUMN_REF',
        alias: '',
        column_names: [`%${value}%`],
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

export const containsTransform = (query: ContainsFilters): ParsedExpression => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple Contains condition
   */
  if (values.length === 1) {
    return containsDuckdbCondition(member, values[0]);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(containsDuckdbCondition(member, value));
  });
  return orCondition;
};
