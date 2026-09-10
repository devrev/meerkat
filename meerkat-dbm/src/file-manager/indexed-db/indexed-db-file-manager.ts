import { TableConfig } from '../../dbm/types';
import { File, Table } from '../../types';
import { isDefined, mergeFileStoreIntoTable } from '../../utils';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { FileRegisterer } from '../file-registerer';
import { BaseIndexedDBFileManager } from './base-indexed-db-file-manager';

// Default max file size is 500mb
const DEFAULT_MAX_FILE_SIZE = 500 * 1024 * 1024;
const INDEXED_DB_FILE_CHUNK_SIZE = 64 * 1024 * 1024;
const FILE_CHUNK_SEPARATOR = '::meerkat-chunk::';

const getChunkFileName = (fileName: string, chunkIndex: number): string =>
  `${fileName}${FILE_CHUNK_SEPARATOR}${chunkIndex}`;

const getStoredFileKeys = (file: File): string[] => [
  file.fileName,
  ...Array.from({ length: file.chunkCount ?? 0 }, (_, chunkIndex) =>
    getChunkFileName(file.fileName, chunkIndex)
  ),
];

const getStoredFiles = (file: File): File[] => {
  if (file.buffer.byteLength <= INDEXED_DB_FILE_CHUNK_SIZE) {
    return [file];
  }

  const chunkCount = Math.ceil(
    file.buffer.byteLength / INDEXED_DB_FILE_CHUNK_SIZE
  );
  const chunks = Array.from({ length: chunkCount }, (_, chunkIndex) => {
    const start = chunkIndex * INDEXED_DB_FILE_CHUNK_SIZE;
    const end = Math.min(
      start + INDEXED_DB_FILE_CHUNK_SIZE,
      file.buffer.byteLength
    );

    return {
      fileName: getChunkFileName(file.fileName, chunkIndex),
      buffer: file.buffer.slice(start, end),
    };
  });

  return [
    {
      fileName: file.fileName,
      buffer: new Uint8Array(),
      chunkCount,
    },
    ...chunks,
  ];
};

export class IndexedDBFileManager
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

  private async replaceStoredFiles(files: File[]): Promise<void> {
    const existingFiles = await this.indexedDB.files.bulkGet(
      files.map((file) => file.fileName)
    );
    const existingKeys = existingFiles
      .filter(isDefined)
      .flatMap(getStoredFileKeys);

    await this.indexedDB.files.bulkDelete(existingKeys);
    await this.indexedDB.files.bulkPut(files.flatMap(getStoredFiles));
  }

  private async getStoredFile(fileName: string): Promise<File | undefined> {
    const storedFile = await this.indexedDB.files.get(fileName);

    if (!storedFile?.chunkCount) {
      return storedFile;
    }

    const chunks = await this.indexedDB.files.bulkGet(
      Array.from({ length: storedFile.chunkCount }, (_, chunkIndex) =>
        getChunkFileName(fileName, chunkIndex)
      )
    );

    if (chunks.some((chunk) => !chunk)) {
      throw new Error(`Missing IndexedDB chunk for file: ${fileName}`);
    }

    const definedChunks = chunks.filter(isDefined);
    const bufferLength = definedChunks.reduce(
      (total, chunk) => total + chunk.buffer.byteLength,
      0
    );
    const buffer = new Uint8Array(bufferLength);
    let offset = 0;

    for (const chunk of definedChunks) {
      buffer.set(chunk.buffer, offset);
      offset += chunk.buffer.byteLength;
    }

    return { fileName, buffer };
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
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

    const newFilesData = fileBuffers.map((fileBuffer): File => {
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

          await this.replaceStoredFiles(newFilesData);
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

          await this.replaceStoredFiles([{ fileName, buffer }]);
        }
      )
      .catch((error) => {
        console.error(error);
      });
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

      const filesData = await Promise.all(
        uniqueFileNames.map((fileName) => this.getStoredFile(fileName))
      );

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

    // Remove the files and any chunks from the IndexedDB
    const storedFiles = await this.indexedDB.files.bulkGet(fileNames);
    const storedFileKeys = storedFiles
      .filter(isDefined)
      .flatMap(getStoredFileKeys);
    await this.indexedDB.files.bulkDelete(storedFileKeys);
  }

  async onDBShutdownHandler() {
    this.fileRegisterer.flushFileCache();
  }
}
