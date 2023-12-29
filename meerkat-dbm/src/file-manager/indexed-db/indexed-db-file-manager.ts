import { InstanceManagerType } from '../../dbm/instance-manager';
import { mergeFileBufferStoreIntoTable } from '../../utils/merge-file-buffer-store-into-table';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
  Table,
} from '../file-manager-type';
import { DuckDBFilesDatabase } from './duckdb-files-database';
import { FileRegisterer } from './file-registerer';

export class IndexedDBFileManager implements FileManagerType {
  // IndexedDB instance
  private indexedDB: DuckDBFilesDatabase;
  private fileRegisterer: FileRegisterer;
  private configurationOptions: FileManagerConstructorOptions['options'];

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    options,
  }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.instanceManager = instanceManager;
    this.indexedDB = new DuckDBFilesDatabase();
    this.fileRegisterer = new FileRegisterer({ instanceManager });
    this.configurationOptions = options;
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
    fileBuffers: FileBufferStore[],
    tableMetadata?: Record<string, object>
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
      return {
        tableName,
        files: updatedTableMap.get(tableName)?.files ?? [],
        metadata: tableMetadata?.[tableName],
      };
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

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    // Retrieve file data from IndexedDB
    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async getFilesNameForTables(tableNames: string[]): Promise<
    {
      tableName: string;
      files: string[];
    }[]
  > {
    const tableData = await this.indexedDB.tablesKey.bulkGet(tableNames);

    return tableData.map((table) => ({
      tableName: table?.tableName ?? '',
      files: (table?.files ?? []).map((file) => file.fileName),
    }));
  }

  async fileCleanUpIfRequired(tableData: (Table | undefined)[]) {
    // Default max file size is 500mb
    const DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024;
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

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.bulkGet(tableNames);

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
        filesData.map(async (file) => {
          if (file) {
            await this.fileRegisterer.registerFileBuffer(
              file.fileName,
              file.buffer
            );
          }
        })
      );
    });

    await Promise.all(promises);
  }

  /**
   * Get the list of files for the specified table name
   */
  async getTableByName(tableName: string): Promise<Table | undefined> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    return tableData;
  }

  /**
   * Drop the specified files by tableName from the IndexedDB
   */
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
