import { FileBufferStore } from '@devrev/meerkat-dbm';

export const writeBufferToFile = (fileBuffer: FileBufferStore) => {
  const { buffer, fileName, tableName } = fileBuffer;
  const dirPath = `electron-app/src/data/${tableName}`;
  const updatedFileName = `${dirPath}/${fileName}`;

  // Create directory if it doesn't exist
  const fs = require('fs');
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  console.log('writeFile', updatedFileName);

  // Convert buffer to Uint8Array if it isn't already
  const uint8Array =
    buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);

  // Use Node's fs module to write the file
  fs.writeFileSync(updatedFileName, uint8Array);
};

export const readFilesForTable = (tableName: string) => {
  const dirPath = `electron-app/src/data/${tableName}`;
  const fs = require('fs');

  // Check if directory exists
  if (!fs.existsSync(dirPath)) {
    return [];
  }
  // Read all files in the directory
  const files = fs.readdirSync(dirPath);

  // Read the content of each file and return as array of buffers
  return files.map((fileName) => {
    const filePath = `${dirPath}/${fileName}`;
    return {
      name: `${fileName}`,
      buffer: fs.readFileSync(filePath),
    };
  });
};
