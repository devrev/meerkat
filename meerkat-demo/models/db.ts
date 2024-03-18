import { TableSchema } from '@devrev/meerkat-core';
import Dexie, { Table } from 'dexie';

interface DuckDBFile {
  id?: number;
  name: string;
  //   blob: ArrayBuffer;
  fileType: 'csv' | 'json';
}

interface DataSource {
  id?: number;
  name: string;
  files: string[];
  data: TableSchema;
}

export class MeerkatDemoIndexDB extends Dexie {
  files!: Table<DuckDBFile, number>;
  dataSources!: Table<DataSource, number>;
  constructor() {
    super('MeerkatDemoIndexDB');
    this.version(1).stores({
      files: '++id',
      dataSources: '++id',
    });
  }
}

export const db = new MeerkatDemoIndexDB();
