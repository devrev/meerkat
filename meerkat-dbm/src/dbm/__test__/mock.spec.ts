import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerType,
} from '../../file-manager/file-manager-type';
import { FileData, Table, TableWiseFiles } from '../../types';
import { InstanceManagerType } from '../instance-manager';
import { TableConfig } from '../types';

export const mockDB = {
  registerFileBuffer: async (name: string, buffer: Uint8Array) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(buffer);
      }, 200);
    });
  },
  connect: async () => {
    return {
      query: async (query: string) => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve([query]);
          }, 200);
        });
      },
      cancelSent: async () => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve(true);
          }, 100);
        });
      },
      close: async () => {
        // do nothing
      },
    };
  },
};

export class InstanceManager implements InstanceManagerType {
  async getDB() {
    return mockDB as AsyncDuckDB;
  }

  async terminateDB() {
    // do nothing
  }
}

export class MockFileManager implements FileManagerType {
  private fileBufferStore: Record<string, FileBufferStore> = {};
  private tables: Record<string, Table> = {};

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    for (const prop of props) {
      this.fileBufferStore[prop.fileName] = prop;
      this.tables[prop.tableName] = this.tables[prop.tableName] || {
        files: [],
      };
      this.tables[prop.tableName].files.push(...props);
    }
  }

  async registerFileBuffer(prop: FileBufferStore): Promise<void> {
    this.fileBufferStore[prop.fileName] = prop;
    this.tables[prop.tableName] = this.tables[prop.tableName] || { files: [] };
    this.tables[prop.tableName].files.push(prop);
  }

  async bulkRegisterJSON(props: FileJsonStore[]): Promise<void> {
    for (const prop of props) {
      await this.registerJSON(prop);
    }
  }

  async registerJSON(prop: FileJsonStore): Promise<void> {
    const { json, ...fileData } = prop;

    this.registerFileBuffer({
      ...fileData,
      buffer: new Uint8Array(),
    });
  }

  async getFileBuffer(name: string): Promise<Uint8Array> {
    const fileBuffer = this.fileBufferStore[name];
    if (!fileBuffer) {
      throw new Error(`File buffer for ${name} not found`);
    }
    return fileBuffer.buffer;
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    const tableNames = tables.map((table) => table.name);
    for (const tableName of tableNames) {
      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          // mount operation here
          console.log(`Mounted file buffer for ${key}`);
        }
      }
    }
  }

  async getFilesByTableName(tableName: string): Promise<FileData[]> {
    const files: FileData[] = [];

    for (const key in this.fileBufferStore) {
      if (this.fileBufferStore[key].tableName === tableName) {
        files.push({ fileName: key });
      }
    }

    return files;
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    for (const fileName of fileNames) {
      delete this.fileBufferStore[fileName];
    }
  }

  async getFilesNameForTables(
    tableNames: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    const data: TableWiseFiles[] = [];

    for (const { name: tableName } of tableNames) {
      const files: FileData[] = [];

      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          files.push({ fileName: key });
        }
      }

      data.push({
        tableName,
        files,
      });
    }

    return data;
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    return this.tables[table.name];
  }

  async setTableMetadata(table: string, metadata: object): Promise<void> {
    this.tables[table].metadata = metadata;
  }

  onDBShutdownHandler = jest.fn(async () => {
    // do nothing
  });
}
