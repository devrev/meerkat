import { DuckDBType, DuckDBTypeId, DuckDBValue } from '@duckdb/node-api';

import { TypeInfo } from 'duckdb';
import {
  convertDuckDBValueToJS,
  convertTableDataToJSON,
} from '../duckdb-type-convertor';

const duckDbSimpleTypeConvertorArray: {
  type: TypeInfo;
  input: unknown;
  output: unknown;
}[] = [
  {
    type: {
      id: 'SQLNULL',
      sql_type: 'NULL',
    },
    input: null,
    output: null,
  },
  {
    type: {
      id: 'DATE',
      sql_type: 'DATE',
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    type: {
      id: 'TIMESTAMP',
      sql_type: 'TIMESTAMP',
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    type: {
      id: 'TIME',
      sql_type: 'TIME',
    },
    input: 1648771200000,
    output: '2022-04-01T00:00:00.000Z',
  },
  {
    type: {
      id: 'FLOAT',
      sql_type: 'FLOAT',
    },
    input: 1.23,
    output: 1.23,
  },
  {
    type: {
      id: 'BIGINT',
      sql_type: 'BIGINT',
    },
    input: BigInt(123),
    output: 123,
  },
  {
    type: {
      id: 'INTEGER',
      sql_type: 'INTEGER',
    },
    input: 123,
    output: 123,
  },
  {
    type: {
      id: 'DECIMAL',
      sql_type: 'DECIMAL',
    },
    input: '1.23',
    output: 1.23,
  },
  {
    type: {
      id: 'BOOLEAN',
      sql_type: 'BOOLEAN',
    },
    input: true,
    output: true,
  },
  {
    type: {
      id: 'VARCHAR',
      sql_type: 'VARCHAR',
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
        convertDuckDBValueToJS(item.type as TypeInfo, item.input)
      ).toStrictEqual(item.output);
    });
  });

  it('should convert complex types correctly', () => {
    duckDBComplexTypeConvertorArray.forEach((item) => {
      expect(
        convertTableDataToJSON(
          item.data as Record<string, DuckDBValue>[],
          item.columnTypes as { name: string; type: DuckDBType }[]
        )
      ).toStrictEqual(item.output);
    });
  });
});
