import { TableConfig } from '../../dbm/types';
import {
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { IndexedDBFileManager } from './indexed-db-file-manager';

export class ParallelIndexedDBFileManager
  extends IndexedDBFileManager
  implements FileManagerType
{
  constructor(options: FileManagerConstructorOptions) {
    super(options);
  }

  override async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    // no-op
  }
}
