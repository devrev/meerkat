import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { Dimension, Measure } from '../../types/cube-types/table';

import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import {
  createColumnRef,
  CreateColumnRefOptions,
  valueBuilder,
} from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const containsDuckdbCondition = (
  columnName: string,
  value: string,
  memberInfo: Measure | Dimension,
  options: CreateColumnRefOptions
) => {
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: '~~*',
    schema: '',
    children: [
      createColumnRef(columnName, options),
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

export const containsTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  // SQL expressions not supported for contains operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(
      member,
      query.sqlExpression,
      'contains',
      options
    );
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  const values = query.values;

  /**
   * If there is only one value, we can create a simple Contains condition
   */
  if (values.length === 1) {
    return containsDuckdbCondition(member, values[0], memberInfo, options);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      containsDuckdbCondition(member, value, memberInfo, options)
    );
  });
  return orCondition;
};
