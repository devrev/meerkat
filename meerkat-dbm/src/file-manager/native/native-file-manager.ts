import { NativeBridge } from '../../dbm/dbm-native/native-bridge';
import { TableConfig } from '../../dbm/types';
import { DBMLogger } from '../../logger';
import { DBMEvent } from '../../logger/event-types';
import { getBufferFromJSON, mergeFileBufferStoreIntoTable } from '../../utils';
import {
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
  RemoteFileStore,
} from '../file-manager-type';
import { BaseIndexedDBFileManager } from '../indexed-db/base-indexed-db-file-manager';

export class NativeFileManager
  extends BaseIndexedDBFileManager<RemoteFileStore>
  implements FileManagerType<RemoteFileStore>
{
  private nativeManager: NativeBridge;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

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
    this.logger = logger;
    this.onEvent = onEvent;
  }

  async bulkRegisterFileNative(remoteFiles: RemoteFileStore[]): Promise<void> {
    //no op
  }

  async registerFileNative(remoteFile: RemoteFileStore): Promise<void> {
    //no op
  }

  async bulkRegisterFileBuffer(fileBuffers: RemoteFileStore[]): Promise<void> {
    const nativeFileBuffers = fileBuffers.filter(
      (fileBuffer): fileBuffer is RemoteFileStore => 'fileUrl' in fileBuffer
    );

    const tableNames = Array.from(
      new Set(nativeFileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      nativeFileBuffers,
      currentTableData
    );

    /**
     * Extracts the tables and files data from the tablesMap and fileBuffers
     * in format that can be stored in IndexedDB
     */
    const updatedTableData = tableNames.map((tableName) => {
      return { tableName, files: updatedTableMap.get(tableName)?.files ?? [] };
    });

    const newFilesData = nativeFileBuffers.map((fileBuffer) => {
      return {
        fileUrl: fileBuffer.fileUrl,
        fileName: fileBuffer.fileName,
        tableName: fileBuffer.tableName,
      };
    });

    // Update the tables in IndexedDB and register the files in Native File System
    await this.indexedDB.tablesKey.bulkPut(updatedTableData);

    await this.nativeManager.registerFiles({ files: newFilesData });
  }

  override async registerFileBuffer(file: RemoteFileStore): Promise<void> {
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

  async bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void> {
    const fileBuffers = await Promise.all(
      jsonData.map(async (jsonFile) => {
        const { json, tableName, ...fileData } = jsonFile;

        const bufferData = await getBufferFromJSON({
          instanceManager: this.instanceManager,
          json: json,
          tableName,
          logger: this.logger,
          onEvent: this.onEvent,
          metadata: jsonFile.metadata,
        });

        return { buffer: bufferData, tableName, ...fileData };
      })
    );

    await this.bulkRegisterFileBuffer(fileBuffers);
  }

  async registerJSON(jsonData: FileJsonStore): Promise<void> {
    const { json, tableName, ...fileData } = jsonData;

    /**
     * Convert JSON to buffer
     */
    const bufferData = await getBufferFromJSON({
      instanceManager: this.instanceManager,
      json,
      tableName,
      logger: this.logger,
      onEvent: this.onEvent,
      metadata: jsonData.metadata,
    });

    /**
     * Register the buffer in the file manager
     */
    await this.registerFileBuffer({
      buffer: bufferData,
      tableName,
      ...fileData,
    });
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
