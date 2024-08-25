import { Field, Type, vectorFromArray } from 'apache-arrow';

import { convertArrowValueToJS } from './duck-type-convertor';

const FIELD = {
  metadata: {},
  name: 'null',
  nullable: true,
  type: {
    children: [{ metadata: {}, name: 'null', nullable: true, typeId: Type.Utf8 }],
  },
};

const NULL_FIELD = {
  ...FIELD,
  typeId: Type.Null,
} as Field;

const UTF8_FIELD = {
  ...FIELD,
  typeId: Type.Utf8,
} as Field;

const FLOAT_FIELD = {
  ...FIELD,
  typeId: Type.Float,
} as Field;

const INT_FIELD = {
  ...FIELD,
  typeId: Type.Int,
} as Field;

const INT64_FIELD = {
  ...FIELD,
  typeId: Type.Int64,
} as Field;

const DECIMAL_FIELD = {
  ...FIELD,
  typeId: Type.Decimal,
} as Field;

const LIST_INT_FIELD = {
  ...FIELD,
  type: {
    children: [{ metadata: {}, name: 'null', nullable: true, typeId: Type.Int }],
  },
  typeId: Type.List,
} as Field;

const LIST_UTF8_FIELD = {
  ...FIELD,
  type: {
    children: [{ metadata: {}, name: 'null', nullable: true, typeId: Type.Utf8 }],
  },
  typeId: Type.List,
} as Field;

const duckDbSimpleTypeConvertorArray = [
  {
    field: NULL_FIELD,
    input: null,
    output: null,
  },
  {
    field: UTF8_FIELD,
    input: 'test',
    output: 'test',
  },
  {
    field: FLOAT_FIELD,
    input: 1.234234,
    output: 1.234234,
  },
  {
    field: INT_FIELD,
    input: 234234,
    output: 234234,
  },
  {
    field: INT64_FIELD,
    input: '234234n',
    output: 234234,
  },
  {
    field: DECIMAL_FIELD,
    input: new Uint8Array([1.34534]),
    output: 1,
  },
  {
    field: DECIMAL_FIELD,
    input: 1.234234234,
    output: 1.234234234,
  },
];

const duckDBComplexTypeConvertorArray = [
  {
    field: LIST_INT_FIELD,
    input: vectorFromArray([1, 2, 3]),
    output: [1, 2, 3],
  },
  {
    field: LIST_UTF8_FIELD,
    input: vectorFromArray(['1', '2', '3']),
    output: ['1', '2', '3'],
  },
];

describe('DuckDBTypeConvertor', () => {
  it('should convert to duckdb to check for simple types', () => {
    duckDbSimpleTypeConvertorArray.forEach((item) => {
      expect(convertArrowValueToJS(item.field, item.input)).toStrictEqual(item.output);
    });
  });

  it('should convert to duckdb to check for complex types', () => {
    duckDBComplexTypeConvertorArray.forEach((item) => {
      expect(convertArrowValueToJS(item.field, item.input)).toStrictEqual(item.output);
    });
  });
});
