import { DEFAULT_CACHE_TIME } from '../constants';
import { FileBufferStore, Table } from '../file-manager/file-manager-type';

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

export const mergeFileBufferStoreIntoTable = (
  fileBufferStore: FileBufferStore[],
  currentTableState: Map<string, Table>
): Map<string, Table> => {
  const tableMap = new Map<string, Table>(currentTableState);

  for (const fileBuffer of fileBufferStore) {
    const { tableName, buffer, ...fileData } = fileBuffer;

    const date = new Date();

    const file = { cacheTime: DEFAULT_CACHE_TIME, date, ...fileData };

    const existingTable = tableMap.get(tableName);

    /**
     * Check if the table already exists in the map if it does, then update the existing table entry
     */
    if (existingTable) {
      const fileExists = existingTable.files.some(
        (file) => file.fileName === fileBuffer.fileName
      );

      if (!fileExists) {
        existingTable.files.push(file);
      }
    } else {
      tableMap.set(fileBuffer.tableName, {
        tableName: fileBuffer.tableName,
        files: [file],
      });
    }
  }

  return tableMap;
};
