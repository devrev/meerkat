import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import {
  convertSharedArrayBufferToUint8Array,
  convertUint8ArrayToSharedArrayBuffer,
  getBufferFromJSON,
  isDefined,
  mergeFileBufferStoreIntoTable,
} from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { FileRegisterer } from '../file-registerer';
import { getFilesByPartition } from '../partition';
import { MeerkatDatabase } from './meerkat-database';

export interface ParallelIndexedDBFileManagerType {
  /**
   *
   * @description
   * Retrieves the buffer data for the tables.
   * @param tables - An array of tables.
   * @returns An array of FileBufferStore objects.
   */
  getTableBufferData?: (
    tables: TableConfig[]
  ) => Promise<FileBufferStore<SharedArrayBuffer>[]>;
}

export class ParallelIndexedDBFileManager
  implements
    ParallelIndexedDBFileManagerType,
    FileManagerType<SharedArrayBuffer>
{
  private indexedDB: MeerkatDatabase; // IndexedDB instance
  private instanceManager: InstanceManagerType;
  private fileRegisterer: FileRegisterer;
  private configurationOptions: FileManagerConstructorOptions['options'];

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private tableFileBuffersMap: Map<
    string,
    FileBufferStore<SharedArrayBuffer>[]
  > = new Map();

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    options,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.indexedDB = new MeerkatDatabase();
    this.instanceManager = instanceManager;
    this.fileRegisterer = new FileRegisterer({ instanceManager });
    this.configurationOptions = options;
    this.logger = logger;
    this.onEvent = onEvent;
  }

  /**
   * Clear all data from the IndexedDB
   */
  private async _flushDB(): Promise<void> {
    await this.indexedDB.tablesKey.clear();
    await this.indexedDB.files.clear();
  }

  async initializeDB(): Promise<void> {
    return;
  }

  async bulkRegisterFileBuffer(
    fileBuffers: FileBufferStore<SharedArrayBuffer>[]
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

    const newFilesData = fileBuffers.map((fileBuffer) => {
      const uint8BufferArray = convertSharedArrayBufferToUint8Array(
        fileBuffer.buffer
      );

      return { buffer: uint8BufferArray, fileName: fileBuffer.fileName };
    });

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.bulkPut(updatedTableData);

          await this.indexedDB.files.bulkPut(newFilesData);
        }
      )
      .catch((error) => {
        console.error(error);
      });
  }

  async registerFileBuffer(
    fileBuffer: FileBufferStore<SharedArrayBuffer>
  ): Promise<void> {
    const { buffer, fileName, tableName } = fileBuffer;

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
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

          const uint8BufferArray = convertSharedArrayBufferToUint8Array(buffer);

          await this.indexedDB.files.put({
            fileName,
            buffer: uint8BufferArray,
          });
        }
      )
      .catch((error) => {
        console.error(error);
      });
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

        const sharedArrayBuffer =
          convertUint8ArrayToSharedArrayBuffer(bufferData);

        return { buffer: sharedArrayBuffer, tableName, ...fileData };
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

    const sharedArrayBuffer = convertUint8ArrayToSharedArrayBuffer(bufferData);

    /**
     * Register the buffer in the file manager
     */
    await this.registerFileBuffer({
      buffer: sharedArrayBuffer,
      tableName,
      ...fileData,
    });
  }

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    // Retrieve file data from IndexedDB
    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async getFilesNameForTables(
    tables: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    const tableNames = tables.map((table) => table.name);

    const tableData = (await this.indexedDB.tablesKey.bulkGet(tableNames))
      .filter(isDefined)
      .reduce((tableObj, table) => {
        tableObj[table.tableName] = table;
        return tableObj;
      }, {} as { [key: string]: Table });

    return tables.map((table) => ({
      tableName: table.name,
      files: getFilesByPartition(
        tableData[table.name]?.files ?? [],
        table.partitions
      ),
    }));
  }

  // async getTableBufferData(tables: TableConfig[]) {}

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    // no-op
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    const tableData = await this.indexedDB.tablesKey.get(table.name);

    if (!tableData) return undefined;

    return {
      ...tableData,
      files: getFilesByPartition(tableData?.files ?? [], table.partitions),
    };
  }

  async getTableBufferData(
    tables: TableConfig[]
  ): Promise<FileBufferStore<SharedArrayBuffer>[]> {
    const tableNames = tables.map((table) => table.name);

    const tablesNotInCache = tableNames.filter(
      (tableName) => !this.tableFileBuffersMap.has(tableName)
    );

    const tableData = await this.getFilesNameForTables(tables);

    const fileBuffers = tableData.map(async (table) => {
      // Retrieve file names for the specified table
      if (this.tableFileBuffersMap.has(table.tableName)) {
        return this.tableFileBuffersMap.get(table.tableName) ?? [];
      }

      const filesData = await this.indexedDB?.files.bulkGet(_filesList);

      // Register file buffers from IndexedDB for each table

      return filesData.filter(isDefined).map(async (file) => {
        await this.fileRegisterer.registerFileBuffer(
          file.fileName,
          file.buffer
        );
      });
    });

    return [];
  }

  async setTableMetadata(tableName: string, metadata: object): Promise<void> {
    await this.indexedDB.tablesKey.update(tableName, {
      metadata,
    });
  }

  async dropFilesByTableName(
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
    await this.indexedDB.files.bulkDelete(fileNames);
  }

  async onDBShutdownHandler() {
    this.fileRegisterer.flushFileCache();
  }
}
