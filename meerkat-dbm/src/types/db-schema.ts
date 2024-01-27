/**
 * Meerkat DB
 * It is consists of two tables:
 * 1. Table: It holds the table name and the files associated with it (without the buffer).
 * 2. File: It holds the actual file buffer data.
 */

/**
 * Table schema
 */
export interface Table {
  tableName: string;
  files: FileData[];
  totalSize?: number;
  metadata?: object;
}

export interface FileData {
  fileName: string;
  partitionKey?: string;
  fileType?: FileType;
  size?: number;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
}

export const FILE_TYPES = {
  PARQUET: 'parquet',
} as const;

export type FileType = (typeof FILE_TYPES)[keyof typeof FILE_TYPES];

/**
 * File schema
 */
export interface File {
  fileName: string;
  buffer: Uint8Array;
}
