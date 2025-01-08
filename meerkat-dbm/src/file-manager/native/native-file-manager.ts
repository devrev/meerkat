import { NativeBridge } from '../../dbm/dbm-native/native-bridge';
import { TableConfig } from '../../dbm/types';
import { mergeFileBufferStoreIntoTable } from '../../utils';
import {
  BaseFileStore,
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
  RemoteFileStore,
} from '../file-manager-type';
import { BaseIndexedDBFileManager } from '../indexed-db/base-indexed-db-file-manager';

export class NativeFileManager
  extends BaseIndexedDBFileManager
  implements FileManagerType
{
  private nativeManager: NativeBridge;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    nativeManager,
  }: FileManagerConstructorOptions & { nativeManager: NativeBridge }) {
    super({
      fetchTableFileBuffers,
      instanceManager,
      logger,
      onEvent,
    });

    this.nativeManager = nativeManager;
  }

  /**
   * Update the tables in IndexedDB with the file data
   */
  private async updateIndexedDBWithTableData(
    fileBuffers: BaseFileStore[]
  ): Promise<void> {
    const tableNames = Array.from(
      new Set(fileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      fileBuffers,
      currentTableData
    );

    /**
     * Extracts the tables and files data from the tablesMap and fileBuffers
     * in format that can be stored in IndexedDB
     */
    const updatedTableData = tableNames.map((tableName) => {
      return { tableName, files: updatedTableMap.get(tableName)?.files ?? [] };
    });

    // Update the tables in IndexedDB
    await this.indexedDB.tablesKey.bulkPut(updatedTableData);
  }

  async bulkRegisterRemoteFile(remoteFiles: RemoteFileStore[]): Promise<void> {
    // Download the files from the remote file system
    await this.nativeManager.downloadFiles({ files: remoteFiles });

    // Update the tables in IndexedDB with the file data
    await this.updateIndexedDBWithTableData(remoteFiles);
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    // Register the files in the native file system
    await this.nativeManager.registerFiles({ files: fileBuffers });

    // Update the tables in IndexedDB with the file data
    await this.updateIndexedDBWithTableData(fileBuffers);
  }

  async registerRemoteFile(file: RemoteFileStore): Promise<void> {
    const { tableName } = file;

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [file],
      currentTableData
    );

    await this.indexedDB.tablesKey.put({
      tableName,
      files: updatedTableMap.get(tableName)?.files ?? [],
    });

    await this.nativeManager.downloadFiles({ files: [file] });
  }

  override async registerFileBuffer(file: FileBufferStore): Promise<void> {
    const { tableName } = file;

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [file],
      currentTableData
    );

    // Update the tables and files table in IndexedDB
    await this.indexedDB.tablesKey.put({
      tableName: file.tableName,
      files: updatedTableMap.get(tableName)?.files ?? [],
    });

    await this.nativeManager.registerFiles({ files: [file] });
  }

  override async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    //no op
  }

  override async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    if (tableData) {
      // Retrieve the files that are not dropped
      const updatedFiles = tableData.files.filter(
        (file) => !fileNames.includes(file.fileName)
      );

      await this.indexedDB.tablesKey.put({
        tableName,
        files: updatedFiles,
      });
    }

    // Remove the files from the IndexedDB
    await this.nativeManager.dropFilesByTableName({ tableName, fileNames });
  }

  override async onDBShutdownHandler(): Promise<void> {
    //no op
  }
}
