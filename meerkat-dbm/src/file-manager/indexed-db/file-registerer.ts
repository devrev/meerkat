import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import { InstanceManagerType } from '../../dbm/instance-manager';

interface FileRegistererConstructorOptions {
  instanceManager: InstanceManagerType;
}

export interface FileRegistererType {
  registerFileBuffer: AsyncDuckDB['registerFileBuffer'];
  registerEmptyFileBuffer: AsyncDuckDB['registerEmptyFileBuffer'];
  getFileBufferFromJson: (
    json: object,
    tableName: string
  ) => Promise<Uint8Array>;
  isFileRegisteredInDB: (fileName: string) => boolean;
  flushFileCache: () => void;
  totalByteLength: () => number;
  getAllFilesInDB: () => string[];
}

export class FileRegisterer implements FileRegistererType {
  instanceManager: InstanceManagerType;

  private registeredFilesSet = new Map<
    string,
    {
      byteLength: number;
    }
  >();

  constructor({ instanceManager }: FileRegistererConstructorOptions) {
    this.instanceManager = instanceManager;
  }

  registerFileBuffer: AsyncDuckDB['registerFileBuffer'] = async (
    fileName,
    buffer
  ) => {
    if (this.registeredFilesSet.has(fileName)) {
      return;
    }
    this.registeredFilesSet.set(fileName, {
      byteLength: buffer.byteLength,
    });
    return this.instanceManager
      .getDB()
      .then((db) => db.registerFileBuffer(fileName, buffer));
  };

  registerEmptyFileBuffer: AsyncDuckDB['registerEmptyFileBuffer'] = async (
    fileName
  ) => {
    await this.instanceManager
      .getDB()
      .then((db) => db.registerEmptyFileBuffer(fileName));

    this.registeredFilesSet.delete(fileName);
  };

  getFileBufferFromJson = async (json: object, tableName: string) => {
    const db = await this.instanceManager.getDB();
    const connection = await db.connect();

    await db.registerFileText(`${tableName}.json`, JSON.stringify(json));

    await connection.insertJSONFromPath(`${tableName}.json`, {
      name: tableName,
    });
    await connection.query("COPY taxi1 TO 'taxi.parquet' (FORMAT PARQUET)';");

    const buffer = await db.copyFileToBuffer('taxi.parquet');

    return buffer;
  };

  isFileRegisteredInDB(fileName: string): boolean {
    return this.registeredFilesSet.has(fileName);
  }

  /**
   * This function on purpose does not clear the files from the DB
   * As this is called when the DB is shutdown and we just want to tell our file registerer to clear the cache
   */
  flushFileCache(): void {
    this.registeredFilesSet.clear();
  }

  totalByteLength(): number {
    return Array.from(this.registeredFilesSet.values()).reduce(
      (total, { byteLength }) => total + byteLength,
      0
    );
  }

  getAllFilesInDB(): string[] {
    return Array.from(this.registeredFilesSet.keys());
  }
}
