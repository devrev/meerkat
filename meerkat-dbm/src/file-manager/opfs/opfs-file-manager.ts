import { TableConfig } from '../../dbm/types';
import { mergeFileStoreIntoTable } from '../../utils';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { FileRegisterer } from '../file-registerer';
import { BaseIndexedDBFileManager } from '../indexed-db/base-indexed-db-file-manager';

export class OPFSFileManager
  extends BaseIndexedDBFileManager
  implements FileManagerType
{
  private fileRegisterer: FileRegisterer;
  private configurationOptions: FileManagerConstructorOptions['options'];

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    options,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    super({ instanceManager, fetchTableFileBuffers, logger, onEvent });

    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.fileRegisterer = new FileRegisterer({ instanceManager });
    this.configurationOptions = options;
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    const db = await this.instanceManager.getDB();

    const tableNames = Array.from(
      new Set(fileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileStoreIntoTable(
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

    const newFilesData = fileBuffers.map((fileBuffer) => {
      return { buffer: fileBuffer.buffer, fileName: fileBuffer.fileName };
    });

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.bulkPut(updatedTableData);

          await Promise.all(
            newFilesData.map((file) =>
              this.fileRegisterer.registerFileBuffer(file.fileName, file.buffer)
            )
          );
        }
      )
      .catch((error) => {
        console.error(error);
      });
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    const { buffer, fileName, tableName } = fileBuffer;

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileStoreIntoTable(
      [fileBuffer],
      currentTableData
    );

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.put({
            tableName: fileBuffer.tableName,
            files: updatedTableMap.get(tableName)?.files ?? [],
          });

          await this.fileRegisterer.registerFileBuffer(fileName, buffer);
        }
      )
      .catch((error) => {
        console.error(error);
      });
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    const tableData = await this.getFilesNameForTables(tables);

    /**
     * Check if the file registered size is not more than the limit
     * If it is more than the limit, then remove the files which are not needed while mounting this the tables
     */
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);
  }

  async onDBShutdownHandler() {
    this.fileRegisterer.flushFileCache();
  }
}
