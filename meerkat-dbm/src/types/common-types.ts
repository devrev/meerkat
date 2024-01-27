import { FileData } from './db-schema';

export type TableWiseFiles = {
  tableName: string;
  files: FileData[];
};
