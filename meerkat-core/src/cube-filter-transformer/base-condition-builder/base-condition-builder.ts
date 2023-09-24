import { Dimension, Measure } from '@devrev/cube-types';
import {
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { CUBE_TYPE_TO_DUCKDB_TYPE } from '../../utils/cube-type-to-duckdb-type';
import { convertFloatToInt, getTypeInfo } from '../../utils/get-type-info';

export const baseDuckdbCondition = (
  columnName: string,
  type: ExpressionType,
  value: string,
  memberInfo: Measure | Dimension
) => {
  return {
    class: ExpressionClass.COMPARISON,
    type: type,
    alias: '',
    left: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [columnName],
    },
    right: {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: valueBuilder(value, memberInfo),
    },
  };
};

export const valueBuilder = (
  value: string,
  memberInfo: Measure | Dimension
) => {
  switch (memberInfo.type) {
    case 'string':
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: value,
      };

    case 'number': {
      const parsedValue = parseFloat(value);
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: getTypeInfo(parsedValue),
        },
        is_null: false,
        value: convertFloatToInt(parsedValue),
      };
    }
    case 'boolean': {
      const parsedValue = value === 'true';
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: parsedValue,
      };
    }
    case 'time':
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE[memberInfo.type],
          type_info: null,
        },
        is_null: false,
        value: value,
      };

    default:
      return {
        type: {
          id: CUBE_TYPE_TO_DUCKDB_TYPE.string,
          type_info: null,
        },
        is_null: false,
        value: value,
      };
  }
};
