export interface Table {
  tableName: string;
  files: FileData[];
  totalSize?: number;
  metadata?: object;
}

export interface FileData {
  fileName: string;
  fileType?: FileType;
  size?: number;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
}

export interface File {
  fileName: string;
  buffer: Uint8Array;
}

export const FILE_TYPES = {
  PARQUET: 'parquet',
} as const;

export type FileType = (typeof FILE_TYPES)[keyof typeof FILE_TYPES];
