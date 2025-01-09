import { FileStore } from '../file-manager';
import { Table } from '../types';
/**
 * Merges an array of fileStore objects into the current state of tables.
 * The function handles scenarios where tables and file buffers may need to be added or updated.
 *
 * @example
 * const currentTableState = new Map<string, Table>();
 * const fileStore = [
 *   { tableName: 'taxi1', fileName: 'taxi1.parquet', buffer: new Uint8Array(0) },
 *   { tableName: 'taxi1', fileName: 'taxi2.parquet', buffer: new Uint8Array(0) },
 *   { tableName: 'taxi2', fileName: 'taxi2.parquet', buffer: new Uint8Array(0) },
 * ];
 * const updatedTableMap = mergeFileStoreIntoTable(fileStore, currentTableState);
 *
 * //returns  Map {
 *  'taxi1' => { tableName: 'taxi1', files: [{ fileName: 'taxi1.parquet' }, { fileName: 'taxi2.parquet' }] },
 *  'taxi2' => { tableName: 'taxi2', files: [{ fileName: 'taxi2.parquet' }] },
 *  }
 *
 */

type OmitBufferAndJson<FileStore> = Omit<FileStore, 'buffer' | 'json'>;

const omitDataProperties = (obj: FileStore): OmitBufferAndJson<FileStore> => {
  const { buffer, json, ...rest } = obj as any;
  return rest;
};

export const mergeFileStoreIntoTable = (
  files: FileStore[],
  currentTableState: Table[]
): Map<string, Table> => {
  const tableMap = new Map<string, Table>(
    currentTableState.map((table) => [table.tableName, { ...table }])
  );

  for (const file of files) {
    const { tableName, ...fileData } = omitDataProperties(file);

    const existingTable = tableMap.get(tableName);

    if (existingTable) {
      const existingFileIndex = existingTable.files.findIndex(
        (file) => file.fileName === fileData.fileName
      );

      if (existingFileIndex !== -1) {
        existingTable.files[existingFileIndex] = {
          ...existingTable.files[existingFileIndex],
          ...fileData,
        };
      } else {
        existingTable.files.push(fileData);
      }
    } else {
      tableMap.set(file.tableName, {
        tableName: file.tableName,
        files: [fileData],
      });
    }
  }

  return tableMap;
};
