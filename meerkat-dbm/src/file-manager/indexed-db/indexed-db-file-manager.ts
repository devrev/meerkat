import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import {
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

// Default max file size is 500mb
const DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024;

export class IndexedDBFileManager implements FileManagerType {
  private indexedDB: MeerkatDatabase; // IndexedDB instance
  private instanceManager: InstanceManagerType;
  private fileRegisterer: FileRegisterer;
  private configurationOptions: FileManagerConstructorOptions['options'];

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;

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

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
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

          await this.indexedDB.files.bulkPut(newFilesData);
        }
      )
      .catch((error) => {
        console.error(error);
      });
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
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

          await this.indexedDB.files.put({ fileName, buffer });
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
