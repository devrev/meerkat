import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { Dimension, Measure } from '../../types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { andDuckdbCondition } from '../and/and';
import { containsDuckdbCondition } from '../contains/contains';
import { CubeToParseExpressionTransform } from '../factory';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const notContainsDuckdbCondition = (
  columnName: string,
  value: string | number,
  memberInfo: Measure | Dimension,
  options: CreateColumnRefOptions
) => {
  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.OPERATOR_NOT,
    alias: '',
    children: [containsDuckdbCondition(columnName, value, memberInfo, options)],
  };
};

export const notContainsTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(
      member,
      query.sqlExpression,
      'notContains',
      options
    );
  }

  if (!query.values || query.values.length === 0) {
    throw new Error('Contains filter must have at least one value');
  }

  const values = query.values;

  if (values.length === 1) {
    return notContainsDuckdbCondition(member, values[0], memberInfo, options);
  }

  // Exclude every supplied value: NOT contains(a) AND NOT contains(b).
  const andCondition = andDuckdbCondition();
  values.forEach((value) => {
    andCondition.children.push(
      notContainsDuckdbCondition(member, value, memberInfo, options)
    );
  });
  return andCondition;
};
