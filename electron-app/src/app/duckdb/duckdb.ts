import { FileBufferStore } from '@devrev/meerkat-dbm';
import { QueryOptions, TableConfig } from 'meerkat-dbm/src/dbm/types';
import { getDBInstance } from './duckdb-instance';
import { readFilesForTable, writeBufferToFile } from './utils';

export class NodeDuckDBFileManager {
  constructor() {}

  /**
   * Clear all data from the IndexedDB
   */

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    writeBufferToFile(fileBuffer);
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    tables.forEach((table) => {
      try {
        const files = readFilesForTable(table.name);

        files.forEach(async (file) => {
          try {
            const db = await getDBInstance();
            console.log(
              `CREATE TABLE IF NOT EXISTS ${table.name} AS SELECT * FROM read_parquet(['${file.name}']);`
            );
            db.run(
              `CREATE TABLE IF NOT EXISTS ${table.name} AS SELECT * FROM read_parquet(['${file.name}']);`
            );
          } catch (error) {
            console.log(
              `Failed to register buffer for file ${file.name}:`,
              error
            );
            throw error;
          }
        });
      } catch (error) {
        console.log(`Failed to read files for table ${table.name}:`, error);
        throw error;
      }
    });
  }

  async executeQuery({
    query,
    tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }): Promise<any> {
    await this.mountFileBufferByTables(tables);

    const db = await getDBInstance();

    const result = await db.run(query);

    const rows = await result.getRows();
    console.log('rows', result);
    return rows;
  }
}

const duckDB = new NodeDuckDBFileManager();

export default duckDB;
