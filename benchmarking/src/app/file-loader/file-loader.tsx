import axios from 'axios';
import { useState } from 'react';

import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { generateViewQuery } from '../utils';
import { TAXI_FILE_URL } from './constants';

// Write a file directly to OPFS root
async function writeInOPFS(fileName: string, buffer: Uint8Array) {
  try {
    const root = await navigator.storage.getDirectory();
    const fileHandle = await root.getFileHandle(fileName, { create: true });
    const writable = await fileHandle.createWritable();
    await writable.write(buffer);
    await writable.close();
    console.log('Successfully wrote file to OPFS root:', fileName);
  } catch (err) {
    console.error('Error writing to OPFS root:', err);
  }
}

export const FileLoader = ({ children }: { children: JSX.Element }) => {
  const { fileManager, fileManagerType, dbm } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      const file = await axios.get(TAXI_FILE_URL, {
        responseType: 'arraybuffer',
      });
      const fileBuffer = file.data;

      const fileBufferView = new Uint8Array(fileBuffer);
      const fileName = 'taxi.parquet';

      // Always write to OPFS at the root
      await writeInOPFS(fileName, fileBufferView);

      // Register file buffer with the file manager
      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'opfs://taxi.parquet',
        buffer: fileBufferView,
      });

      // Create views for raw and memory file manager after registering the files
      if (
        fileManagerType === 'raw' ||
        fileManagerType === 'memory' ||
        fileManagerType === 'opfs'
      ) {
        await dbm.query(generateViewQuery('taxi', ['opfs://taxi.parquet']));
        // await dbm.query(generateViewQuery('taxi_json', ['taxi_json.parquet']));
      }

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
