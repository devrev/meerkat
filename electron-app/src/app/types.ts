export enum NativeAppEvent {
  REGISTER_FILES = 'register-files',
  QUERY = 'query',
  DROP_FILES_BY_TABLE_NAME = 'drop-files-by-table-name',
  GET_FILE_PATHS_FOR_TABLE = 'get-file-paths-for-table',
}

export interface DropTableFilesPayload {
  tableName: string;
  files: string[];
}
