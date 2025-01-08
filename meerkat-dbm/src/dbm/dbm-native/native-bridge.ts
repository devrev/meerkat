import { Schema } from 'apache-arrow';

export interface FileMetadata {
  tableName: string;
  fileName: string;
  fileUrl: string;
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
   * @param fileUrls - The file urls to download and register.
   */
  registerFiles({ files }: { files: FileMetadata[] }): Promise<void>;

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

  dropFilesByTableName({
    tableName,
    fileNames,
  }: {
    tableName: string;
    fileNames: string[];
  }): Promise<void>;
}
