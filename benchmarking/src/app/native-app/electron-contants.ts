import { FileStore } from '@devrev/meerkat-dbm';

export type ContextBridgeApi = {
  registerFiles: (files: FileStore[]) => Promise<void>;
  query: (query: string) => Promise<Record<string, unknown>>;
  getFilePathsForTable: (tableName: string) => Promise<string[]>;
  dropFilesByTableName: (tableData: {
    tableName: string;
    fileNames: string[];
  }) => Promise<void>;
};

declare global {
  interface Window {
    api: ContextBridgeApi;
  }
}
