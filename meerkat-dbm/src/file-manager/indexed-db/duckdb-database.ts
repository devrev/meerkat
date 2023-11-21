import Dexie from 'dexie';
import { Dataset, File } from '../file-manager-type';

export class DuckDBDatabase extends Dexie {
  datasets: Dexie.Table<Dataset, string>;
  files: Dexie.Table<File, string>;

  constructor() {
    super('DuckDBDatabase');

    this.version(1).stores({
      datasets: '&tableName, files, size, metadata',
      files: '&fileName, metadata',
    });

    this.datasets = this.table('datasets');
    this.files = this.table('files');
  }
}
