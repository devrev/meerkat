import { NativeBridge } from '../../dbm/dbm-native/native-bridge';
import { mergeFileStoreIntoTable } from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
  FileStore,
  FileUrlStore,
} from '../file-manager-type';
import { BaseIndexedDBFileManager } from '../indexed-db/base-indexed-db-file-manager';

export class NativeFileManager
  extends BaseIndexedDBFileManager
  implements FileManagerType
{
  private readonly nativeBridge: NativeBridge;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    nativeBridge,
  }: FileManagerConstructorOptions & { nativeBridge: NativeBridge }) {
    super({
      fetchTableFileBuffers,
      instanceManager,
      logger,
      onEvent,
    });

    this.nativeBridge = nativeBridge;
  }

  private async updateIndexedDBWithTableData(
    files: FileStore[]
  ): Promise<void> {
    const tableNames = [...new Set(files.map((file) => file.tableName))];

    const currentTables = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileStoreIntoTable(files, currentTables);

    /**
     * Extracts the tables and files data from the tablesMap and fileBuffers
     * in format that can be stored in IndexedDB
     */
    const updatedTableData = tableNames.map((tableName) => ({
      tableName,
      files: updatedTableMap.get(tableName)?.files ?? [],
    }));

    await this.indexedDB.tablesKey.bulkPut(updatedTableData);
  }

  async bulkRegisterFileUrl(files: FileUrlStore[]): Promise<void> {
    await this.nativeBridge.registerFiles(
      files.map((file) => ({ ...file, type: 'url' }))
    );

    await this.updateIndexedDBWithTableData(files);
  }

  async bulkRegisterJSON(files: FileJsonStore[]): Promise<void> {
    await this.nativeBridge.registerFiles(
      files.map((file) => ({ ...file, type: 'json' }))
    );

    await this.updateIndexedDBWithTableData(files);
  }

  async registerFileUrl(file: FileUrlStore): Promise<void> {
    await this.nativeBridge.registerFiles([{ ...file, type: 'url' }]);

    await this.updateIndexedDBWithTableData([file]);
  }

  async registerJSON(file: FileJsonStore): Promise<void> {
    await this.nativeBridge.registerFiles([{ ...file, type: 'json' }]);

    await this.updateIndexedDBWithTableData([file]);
  }

  override async registerFileBuffer(file: FileBufferStore): Promise<void> {
    // no-op
  }

  async bulkRegisterFileBuffer(): Promise<void> {
    // no-op
  }

  override async mountFileBufferByTables(): Promise<void> {
    // no-op
  }

  override async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    if (tableData) {
      const updatedFiles = tableData.files.filter(
        (file) => !fileNames.includes(file.fileName)
      );

      await this.indexedDB.tablesKey.put({
        tableName,
        files: updatedFiles,
      });
    }

    await this.nativeBridge.dropFilesByTableName({ tableName, fileNames });
  }

  override async onDBShutdownHandler(): Promise<void> {
    // no-op
  }
}
