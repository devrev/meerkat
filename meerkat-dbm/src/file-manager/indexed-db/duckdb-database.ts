import Dexie from 'dexie';
import { File, Table } from '../file-manager-type';

export class DuckDBDatabase extends Dexie {
  datasets: Dexie.Table<Table, string>;
  files: Dexie.Table<File, string>;

  constructor() {
    super('DuckDBDatabase');

    this.version(1).stores({
      datasets: '&tableName, files',
      files: '&fileName',
    });

    this.datasets = this.table('datasets');
    this.files = this.table('files');
  }
}
