import { promises as fs } from 'fs';
import * as path from 'path';
import { BASE_DIR } from './constants';

interface FileBufferStore {
  buffer: Buffer | Uint8Array;
  fileName: string;
  tableName: string;
}

export class FileManager {
  constructor(private readonly baseDir = './storage') {
    fs.mkdir(baseDir, { recursive: true }).catch(console.error);
  }

  private getPath(tableName: string, fileName?: string): string {
    return fileName
      ? path.join(this.baseDir, tableName, fileName)
      : path.join(this.baseDir, tableName);
  }

  async writeFile(file: FileBufferStore): Promise<void> {
    const filePath = this.getPath(file.tableName, file.fileName);

    await fs.mkdir(path.dirname(filePath), { recursive: true });

    await fs.writeFile(filePath, file.buffer);
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

export const fileManager = new FileManager(BASE_DIR);
