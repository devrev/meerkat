import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { IndexedDBFileManager } from './indexed-db-file-manager';

export class RunnerIndexedDBFileManager
  extends IndexedDBFileManager
  implements FileManagerType<Uint8Array>
{
  constructor(options: FileManagerConstructorOptions) {
    super(options);
  }

  override async bulkRegisterFileBuffer(
    fileBuffers: FileBufferStore[]
  ): Promise<void> {
    // no-op
  }

  override async registerFileBuffer(
    fileBuffer: FileBufferStore
  ): Promise<void> {
    // no-op
  }

  override async bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void> {
    // no-op
  }

  override async registerJSON(jsonData: FileJsonStore): Promise<void> {
    // no-op
  }

  override async setTableMetadata(
    tableName: string,
    metadata: object
  ): Promise<void> {
    // no-op
  }

  override async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    // no-op
  }
}
