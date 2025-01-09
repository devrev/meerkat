import { Schema } from 'apache-arrow';

export interface BaseFileStore {
  tableName: string;
  fileName: string;
}

export interface FileUrlStore extends BaseFileStore {
  type: 'url';
  url: string;
}

export interface FileJsonStore extends BaseFileStore {
  type: 'json';
  json: object;
}

export interface FileBufferStore extends BaseFileStore {
  type: 'buffer';
  buffer: Uint8Array;
}

export type FileStore = FileJsonStore | FileUrlStore | FileBufferStore;

export interface QueryResult {
  data?: Record<string, unknown>[];
  schema?: Schema;
}

// Initial interface for the native bridge
export interface NativeBridge {
  /**
   * Register files in the file system.
   * @param files - The files to register.
   */
  registerFiles(files: FileStore[]): Promise<void>;

  /**
   * Query the database.
   * @param query - The query to execute.
   * @returns The result of the query.
   */
  query(query: string): Promise<QueryResult>;

  /**
   * Get the file paths for a table.
   * @param tableName - The table to get the file paths for.
   * @returns The file paths for the table.
   */
  getFilePathsForTable(tableName: string): Promise<string[]>;

  /**
   * Drop specific files from the file system.
   * @param tableName - The table to drop the files from.
   * @param fileNames - The files to drop from the file system.
   */
  dropFilesByTableName({
    tableName,
    fileNames,
  }: {
    tableName: string;
    fileNames: string[];
  }): Promise<void>;
}
