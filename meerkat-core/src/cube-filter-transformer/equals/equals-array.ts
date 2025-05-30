import { COLUMN_NAME_DELIMITER } from 'meerkat-core/src/member-formatters/constants';
import { Dimension, Measure } from 'meerkat-core/src/types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
} from 'meerkat-core/src/types/duckdb-serialization-types/serialization/Expression';
import { ResultModifierType } from 'meerkat-core/src/types/duckdb-serialization-types/serialization/ResultModifier';
import { valueBuilder } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

const equalsDuckDbCondition = (
  columnName: string,
  values: string[],
  memberInfo: Measure | Dimension
) => {
  const columnRef = {
    class: ExpressionClass.COLUMN_REF,
    type: ExpressionType.COLUMN_REF,
    alias: '',
    column_names: columnName.split(COLUMN_NAME_DELIMITER),
  };

  const sqlTreeValues = values.map((value) => {
    const children = {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: valueBuilder(value, memberInfo),
    };
    return children;
  });

  const filterValuesArray = {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: 'list_value',
    schema: 'main',
    children: sqlTreeValues,
    filter: null,
    order_bys: {
      type: ResultModifierType.ORDER_MODIFIER,
      orders: [],
    },
    distinct: false,
    is_operator: false,
    export_state: false,
    catalog: '',
  };

  const sqlTree = {
    class: ExpressionClass.FUNCTION,
    type: ExpressionType.FUNCTION,
    alias: '',
    function_name: 'list_has_all',
    schema: '',
    children: [columnRef, filterValuesArray],
    filter: null,
    order_bys: {
      type: ResultModifierType.ORDER_MODIFIER,
      orders: [],
    },
    distinct: false,
    is_operator: false,
    export_state: false,
    catalog: '',
  };
  return sqlTree;
};

export const equalsArrayTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;
  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (!values) {
    throw new Error('In filter must have at least one value');
  }
  return equalsDuckDbCondition(member, values, memberInfo);
};
