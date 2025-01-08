import { Schema } from 'apache-arrow';

export interface BaseFileMetadata {
  tableName: string;
  fileName: string;
}

export interface FileUrlMetadata extends BaseFileMetadata {
  fileUrl: string;
}

export interface FileBufferMetadata extends BaseFileMetadata {
  buffer: Uint8Array;
}

export interface QueryResult {
  data?: object[];
  schema?: Schema;
  error?: unknown;
}

// Initial interface for the native bridge
export interface NativeBridge {
  /**
   * Download files from the given urls and register them in the file system.
   * @param file - The file urls to download and register.
   */
  downloadFiles({ files }: { files: FileUrlMetadata[] }): Promise<void>;

  /**
   * Register files in the file system.
   * @param files - The files to register.
   */
  registerFiles({ files }: { files: FileBufferMetadata[] }): Promise<void>;

  /**
   * Query the database.
   * @param query - The query to execute.
   * @returns The result of the query.
   */
  query(query: string): Promise<QueryResult>;

  /**
   * Drop specific files from the file system.
   * @param tableName - The table to drop the files from.
   * @param fileNames - The files to drop from the file system.
   */

  dropFilesByTable({
    tableName,
    fileNames,
  }: {
    tableName: string;
    fileNames: string[];
  }): Promise<void>;
}
