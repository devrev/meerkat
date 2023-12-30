import Dexie from 'dexie';
import { File, Table } from '../../types';

/**
 * We don't need to specify the buffer field for index
 * https://dexie.org/docs/Version/Version.stores()#warning
 */

export class MeerkatDatabase extends Dexie {
  tablesKey: Dexie.Table<Table, string>;
  files: Dexie.Table<File, string>;

  constructor() {
    super('MeerkatDatabase');

    this.version(1).stores({
      tablesKey: '&tableName',
      files: '&fileName',
    });

    this.tablesKey = this.table('tablesKey');
    this.files = this.table('files');
  }
}
