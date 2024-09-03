import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import { isDefined } from '../../utils';
import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import { CommunicationInterface } from '../../window-communication/window-communication';
import {
  BaseFileStore,
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { FileRegisterer } from '../file-registerer';
import { getFilesByPartition } from '../partition';
import { MeerkatDatabase } from './meerkat-database';

// Default max file size is 500mb
const DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024;

export class RunnerIndexedDBFileManager implements FileManagerType<Uint8Array> {
  private indexedDB: MeerkatDatabase; // IndexedDB instance
  private fileRegisterer: FileRegisterer;

  private instanceManager: InstanceManagerType;
  private configurationOptions: FileManagerConstructorOptions['options'];
  private communication: CommunicationInterface<BrowserRunnerMessage>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private mountedTablesMap: Map<string, BaseFileStore[]> = new Map();

  constructor({
    instanceManager,
    logger,
    onEvent,
    communication,
  }: FileManagerConstructorOptions & {
    communication: CommunicationInterface<BrowserRunnerMessage>;
  }) {
    this.indexedDB = new MeerkatDatabase();
    this.fileRegisterer = new FileRegisterer({ instanceManager });

    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.communication = communication;
    // this.configurationOptions = configurationOptions;
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

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    // no-op
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    // no-op
  }

  async bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void> {
    // no-op
  }

  async registerJSON(jsonData: FileJsonStore): Promise<void> {
    // no-op
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

  async fileCleanUpIfRequired(tableData: Table[]) {
    const maxFileSize =
      this.configurationOptions?.maxFileSize ?? DEFAULT_MAX_FILE_SIZE;
    const totalByteLengthInDb = this.fileRegisterer.totalByteLength();

    if (totalByteLengthInDb > maxFileSize) {
      const allFilesInDb = this.fileRegisterer.getAllFilesInDB();

      const fileNameToRemove = [];
      for (const table of tableData) {
        for (const file of table?.files ?? []) {
          if (!allFilesInDb.includes(file.fileName)) {
            fileNameToRemove.push(file.fileName);
          }
        }
      }
      for (const fileName of fileNameToRemove) {
        await this.fileRegisterer.registerEmptyFileBuffer(fileName);
      }
    }
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    const tableData = await this.getFilesNameForTables(tables);

    /**
     * Check if the file registered size is not more than the limit
     * If it is more than the limit, then remove the files which are not needed while mounting this the tables
     */
    this.fileCleanUpIfRequired(tableData);

    const promises = tableData.map(async (table) => {
      // Retrieve file names for the specified table
      const _filesList = (table?.files || []).map(
        (fileData) => fileData.fileName
      );

      // Filter out the files that are already registered in DuckDB
      const filesList = _filesList.filter(
        (fileName) => !this.fileRegisterer.isFileRegisteredInDB(fileName)
      );

      const uniqueFileNames = Array.from(new Set(filesList));

      const filesData = await this.indexedDB?.files.bulkGet(uniqueFileNames);

      // Register file buffers from IndexedDB for each table
      await Promise.all(
        filesData.filter(isDefined).map(async (file) => {
          await this.fileRegisterer.registerFileBuffer(
            file.fileName,
            file.buffer
          );
        })
      );
    });

    await Promise.all(promises);
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    const tableData = await this.indexedDB.tablesKey.get(table.name);

    if (!tableData) return undefined;

    return {
      ...tableData,
      files: getFilesByPartition(tableData?.files ?? [], table.partitions),
    };
  }

  async setTableMetadata(tableName: string, metadata: object): Promise<void> {
    // no-op
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    // no-op
  }

  async onDBShutdownHandler() {
    this.fileRegisterer.flushFileCache();
  }
}
