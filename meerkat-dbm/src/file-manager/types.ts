// Datasets table interface
export interface Dataset {
  tableName: string;
  files: FileData[];
  size?: number;
  metadata?: object;
}

interface FileData {
  fileName: string;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
}

// Files table interface
export interface File {
  fileName: string;
  buffer: Uint8Array;
}
