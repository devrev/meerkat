import { FileBufferStore } from '../file-manager/file-manager-type';
import { Table } from '../types';
/**
 * Merges an array of FileBufferStore objects into the current state of tables.
 * The function handles scenarios where tables and file buffers may need to be added or updated.
 *
 * @example
 * const currentTableState = new Map<string, Table>();
 * const fileBufferStore = [
 *   { tableName: 'taxi1', fileName: 'taxi1.parquet', buffer: new Uint8Array(0) },
 *   { tableName: 'taxi1', fileName: 'taxi2.parquet', buffer: new Uint8Array(0) },
 *   { tableName: 'taxi2', fileName: 'taxi2.parquet', buffer: new Uint8Array(0) },
 * ];
 * const updatedTableMap = mergeFileBufferStoreIntoTable(fileBufferStore, currentTableState);
 *
 * //returns  Map {
 *  'taxi1' => { tableName: 'taxi1', files: [{ fileName: 'taxi1.parquet' }, { fileName: 'taxi2.parquet' }] },
 *  'taxi2' => { tableName: 'taxi2', files: [{ fileName: 'taxi2.parquet' }] },
 *  }
 *
 */

export const mergeFileBufferStoreIntoTable = <
  T extends Uint8Array | SharedArrayBuffer
>(
  fileBufferStore: FileBufferStore<T>[],
  currentTableState: Table[]
): Map<string, Table> => {
  const tableMap = new Map<string, Table>(
    currentTableState.map((table) => [table.tableName, { ...table }])
  );

  for (const fileBuffer of fileBufferStore) {
    const { tableName, buffer, ...fileData } = fileBuffer;
    const existingTable = tableMap.get(tableName);

    /**
     * Check if the table already exists in the map if it does, then update the existing table entry
     */
    if (existingTable) {
      const existingFileIndex = existingTable.files.findIndex(
        (file) => file.fileName === fileBuffer.fileName
      );

      if (existingFileIndex !== -1) {
        // If file exists, update the fileData
        existingTable.files[existingFileIndex] = {
          ...existingTable.files[existingFileIndex],
          ...fileData,
        };
      } else {
        // If file does not exist, add it to the files array
        existingTable.files.push(fileData);
      }
    } else {
      tableMap.set(fileBuffer.tableName, {
        tableName: fileBuffer.tableName,
        files: [fileData],
      });
    }
  }

  return tableMap;
};
