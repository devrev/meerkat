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
import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { orDuckdbCondition } from '../or/or';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const containsDuckdbCondition = (
  columnName: string,
  value: string | number,
  memberInfo: Measure | Dimension,
  options: CreateColumnRefOptions
) => {
  const isArray = isArrayTypeMember(memberInfo.type);
  return {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: isArray ? 'list_contains' : 'ilike_escape',
    schema: '',
    children: [
      createColumnRef(columnName, {
        isAlias: options.isAlias,
      }),
      {
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        alias: '',
        value: valueBuilder(
          isArray ? value : `%${String(value).replace(/[\\%_]/g, '\\$&')}%`,
          memberInfo
        ),
      },
      ...(!isArray
        ? [
            {
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              alias: '',
              value: valueBuilder('\\', memberInfo),
            },
          ]
        : []),
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
  };
};

export const containsTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(
      member,
      query.sqlExpression,
      'contains',
      options
    );
  }

  if (!query.values || query.values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  const values = query.values;

  if (values.length === 1) {
    return containsDuckdbCondition(member, values[0], memberInfo, options);
  }

  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      containsDuckdbCondition(member, value, memberInfo, options)
    );
  });
  return orCondition;
};
