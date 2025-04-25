import { COLUMN_NAME_DELIMITER } from '../../member-formatters/constants';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { CubeToParseExpressionTransform } from '../factory';

export const setTransform: CubeToParseExpressionTransform = (query) => {
  const { member } = query;
  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.OPERATOR_IS_NOT_NULL,
    alias: '',
    children: [
      {
        class: 'COLUMN_REF',
        type: 'COLUMN_REF',
        alias: '',
        column_names: member.split(COLUMN_NAME_DELIMITER),
      },
    ],
  };
};
