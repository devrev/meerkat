import { FileBufferStore } from '@devrev/meerkat-dbm';
import * as fs from 'fs';
import { BASE_DIR } from './constants';

class FileManager {
  private baseDir: string;

  constructor(baseDir = BASE_DIR) {
    this.baseDir = baseDir;
  }

  writeBufferToFile(fileBuffer: FileBufferStore) {
    const { buffer, fileName, tableName } = fileBuffer;
    const dirPath = `${this.baseDir}/${tableName}`;
    const updatedFileName = `${dirPath}/${fileName}`;

    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }

    const uint8Array =
      buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    fs.writeFileSync(updatedFileName, uint8Array);

    return updatedFileName;
  }

  getFilePathsForTable(tableName: string) {
    const dirPath = `${this.baseDir}/${tableName}`;

    const fileNames = fs.readdirSync(dirPath);
    const filePaths = fileNames.map((fileName) => `${dirPath}/${fileName}`);

    return filePaths;
  }

  dropFilesByTableNames(tableName: string, files: string[]) {
    const dirPath = `${this.baseDir}/${tableName}`;

    files.forEach((fileName) => {
      const filePath = `${dirPath}/${fileName}`;

      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        console.log(`Deleted file: ${filePath}`);
      } else {
        console.log(`File not found: ${filePath}`);
      }
    });
  }
}

export const fileManager = new FileManager();
