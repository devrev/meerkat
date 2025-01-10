import { FileBufferStore, FileJsonStore } from '@devrev/meerkat-dbm';
import { promises as fs } from 'fs';
import * as path from 'path';
import { BASE_DIR } from './constants';

export class FileManager {
  private readonly baseDir: string;

  constructor(config: { baseDir?: string }) {
    this.baseDir = config.baseDir ?? './data';
    fs.mkdir(this.baseDir, { recursive: true }).catch(console.error);
  }

  public getPath(tableName: string, fileName?: string): string {
    console.log('tableName', tableName, fileName);
    return fileName
      ? path.join(this.baseDir, tableName, fileName)
      : path.join(this.baseDir, tableName);
  }

  async writeFileBuffer(file: FileBufferStore): Promise<void> {
    const filePath = this.getPath(file.tableName, file.fileName);

    await fs.mkdir(path.dirname(filePath), { recursive: true });

    await fs.writeFile(filePath, file.buffer);
  }

  async writeFileJson(file: FileJsonStore): Promise<void> {
    const filePath = this.getPath(file.tableName, file.fileName);

    await fs.mkdir(path.dirname(filePath), { recursive: true });

    await fs.writeFile(filePath, JSON.stringify(file.json));
  }

  async getTableFilePaths(tableName: string): Promise<string[]> {
    try {
      const files = await fs.readdir(this.getPath(tableName));

      return files.map((file) => this.getPath(tableName, file));
    } catch {
      return [];
    }
  }

  async deleteTableFiles(tableName: string, files: string[]): Promise<void> {
    await Promise.all(
      files.map((file) =>
        fs.unlink(this.getPath(tableName, file)).catch(console.error)
      )
    );
  }
}

export const fileManager = new FileManager({ baseDir: BASE_DIR });
