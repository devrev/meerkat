import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { createColumnRef } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

export const notSetTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member } = query;
  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.OPERATOR_IS_NULL,
    alias: '',
    children: [
      createColumnRef(member, {
        isAlias: options.isAlias,
      }),
    ],
  };
};
