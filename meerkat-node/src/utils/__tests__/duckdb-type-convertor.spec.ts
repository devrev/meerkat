import { DuckDBType, DuckDBTypeId, DuckDBValue } from '@duckdb/node-api';

import {
  convertDuckDBValueToJS,
  convertRecordDuckDBValueToJSON,
} from '../duckdb-type-convertor';

const duckDbSimpleTypeConvertorArray = [
  {
    field: {
      typeId: DuckDBTypeId.SQLNULL,
    },
    input: null,
    output: null,
  },
  {
    field: {
      typeId: DuckDBTypeId.DATE,
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    field: {
      typeId: DuckDBTypeId.TIMESTAMP,
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    field: {
      typeId: DuckDBTypeId.TIME,
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    field: {
      typeId: DuckDBTypeId.FLOAT,
    },
    input: 1.23,
    output: 1.23,
  },
  {
    field: {
      typeId: DuckDBTypeId.INTEGER,
    },
    input: 123,
    output: 123,
  },
  {
    field: {
      typeId: DuckDBTypeId.DECIMAL,
    },
    input: '1.23',
    output: 1.23,
  },
  {
    field: {
      typeId: DuckDBTypeId.BOOLEAN,
    },
    input: true,
    output: true,
  },
  {
    field: {
      typeId: DuckDBTypeId.VARCHAR,
    },
    input: 'test',
    output: 'test',
  },
];

const duckDBComplexTypeConvertorArray = [
  {
    columnTypes: [
      {
        name: 'PULocationID',
        type: {
          typeId: DuckDBTypeId.INTEGER,
        },
      },
      {
        name: 'driver_pay',
        type: {
          typeId: DuckDBTypeId.FLOAT,
        },
      },
      {
        name: 'shared_request_flag',
        type: {
          typeId: DuckDBTypeId.VARCHAR,
        },
      },
      {
        name: 'request_datetime',
        type: {
          typeId: DuckDBTypeId.TIMESTAMP,
        },
      },
      {
        name: 'hvfhs_license_num',
        type: {
          typeId: DuckDBTypeId.VARCHAR,
        },
      },
    ],
    data: [
      {
        hvfhs_license_num: 'HV0003',
        request_datetime: 1672531106000,
        PULocationID: 219,
        driver_pay: 37.67,
        shared_request_flag: 'N',
      },
    ],
    output: [
      {
        hvfhs_license_num: 'HV0003',
        request_datetime: '2022-12-31T23:58:26.000Z',
        PULocationID: 219,
        driver_pay: 37.67,
        shared_request_flag: 'N',
      },
    ],
  },
];

describe('DuckDBTypeConvertor', () => {
  it('should convert simple types correctly', () => {
    duckDbSimpleTypeConvertorArray.forEach((item) => {
      expect(
        convertDuckDBValueToJS(item.field as DuckDBType, item.input)
      ).toStrictEqual(item.output);
    });
  });

  it('should convert complex types correctly', () => {
    duckDBComplexTypeConvertorArray.forEach((item) => {
      expect(
        convertRecordDuckDBValueToJSON(
          item.data as Record<string, DuckDBValue>[],
          item.columnTypes as { name: string; type: DuckDBType }[]
        )
      ).toStrictEqual(item.output);
    });
  });
});
