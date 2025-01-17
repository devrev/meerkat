import axios from 'axios';
import { createWriteStream, promises as fs } from 'fs';
import * as path from 'path';

import { encryptString } from '../utils/encrypt-string';

export class FileManager {
  private readonly baseDir: string;

  constructor(config: { baseDir?: string }) {
    this.baseDir = config.baseDir ?? './data';
    fs.mkdir(this.baseDir, { recursive: true }).catch(console.error);
  }

  public getPath(tableName: string, fileName?: string): string {
    return fileName
      ? path.join(this.baseDir, tableName, fileName)
      : path.join(this.baseDir, tableName);
  }

  /**
   * Write a file buffer to the file system.
   */
  async writeFileBuffer(file: {
    tableName: string;
    fileName: string;
    buffer: Uint8Array;
  }): Promise<void> {
    // Hash the file name to avoid file name length issues
    const hashedFileName = encryptString(file.fileName);

    const filePath = this.getPath(file.tableName, hashedFileName);

    await fs.mkdir(path.dirname(filePath), { recursive: true });

    await fs.writeFile(filePath, file.buffer);
  }

  /**
   * Get the file paths for a table.

   */
  async getTableFilePaths(tableName: string): Promise<string[]> {
    try {
      const files = await fs.readdir(this.getPath(tableName));

      return files.map((file) => this.getPath(tableName, file));
    } catch {
      return [];
    }
  }

  /**
   * Delete files from a table.
   */
  async deleteTableFiles(tableName: string, files: string[]): Promise<void> {
    await Promise.all(
      files.map(async (file) => {
        try {
          await fs.unlink(this.getPath(tableName, file));
        } catch (err) {
          console.error(err);
        }
      })
    );
  }

  /**
   * Stream and register a file from a URL.
   * @param params - The parameters for streaming and registering the file.
   * @returns A promise that resolves when the file is streamed and registered.
   */
  async streamAndRegisterFile({
    tableName,
    url,
    headers,
    fileName,
  }: {
    tableName: string;
    url: string;
    headers: Record<string, string>;
    fileName: string;
  }): Promise<void> {
    try {
      const response = await axios({
        headers: {
          ...headers,
        },
        method: 'get',
        responseType: 'stream',
        url,
      });

      const filePath = this.getPath(tableName, fileName);
      await fs.mkdir(path.dirname(filePath), { recursive: true });

      const writer = createWriteStream(filePath);

      return new Promise((resolve, reject) => {
        response.data.pipe(writer);
        writer.on('finish', () => {
          writer.close();
          resolve();
        });
        writer.on('error', (err) => {
          writer.close();
          reject(err);
        });
      });
    } catch (error) {
      console.error('Error streaming file:', error);
      throw error;
    }
  }
}
