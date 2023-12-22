import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import { InstanceManagerType } from '../../dbm/instance-manager';

interface FileRegistererConstructorOptions {
  instanceManager: InstanceManagerType;
}

export class FileRegisterer {
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

  isFileRegisteredInDB(fileName: string): boolean {
    return this.registeredFilesSet.has(fileName);
  }

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
