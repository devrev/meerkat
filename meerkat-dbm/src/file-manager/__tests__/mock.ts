import { jest } from '@jest/globals';

export const mockDB = {
  registerFileBuffer: jest.fn(),
  registerFileText: jest.fn(),
  copyFileToBuffer: jest.fn(() => new Uint8Array([1, 2, 3])),
  registerEmptyFileBuffer: jest.fn(),
  connect: async () => {
    return {
      query: jest.fn(),
      insertJSONFromPath: jest.fn(),
      close: jest.fn(),
    };
  },
};

export const JSON_FILE = {
  tableName: 'taxi-json',
  fileName: 'taxi-json.parquet',
  json: {
    test: 'test',
  },
};

export const JSON_FILES = [
  {
    tableName: 'taxi-json-bulk',
    fileName: 'taxi-json1.parquet',
    json: {
      test: 'test',
    },
  },
  {
    tableName: 'taxi-json-bulk',
    fileName: 'taxi-json2.parquet',
    json: {
      test: 'test',
    },
  },
];
