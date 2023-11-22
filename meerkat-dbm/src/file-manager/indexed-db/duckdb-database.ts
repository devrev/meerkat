import Dexie from 'dexie';
import { File, Table } from '../file-manager-type';

export class DuckDBDatabase extends Dexie {
  // tables is a reserved word in Dexie so we use datasets instead
  datasets: Dexie.Table<Table, string>;
  files: Dexie.Table<File, string>;

  constructor() {
    super('DuckDBDatabase');

    this.version(1).stores({
      datasets: '&tableName, files, metadata',
      files: '&fileName, metadata',
    });

    this.datasets = this.table('datasets');
    this.files = this.table('files');
  }
}
