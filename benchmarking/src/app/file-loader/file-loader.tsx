import axios from 'axios';
import { useState } from 'react';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { generateViewQuery } from '../utils';
import { TAXI_FILE_URL } from './constants';

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

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        buffer: fileBufferView,
      });

      // await fileManager.registerJSON({
      //   json: TAXI_JSON_DATA,
      //   tableName: 'taxi_json',
      //   fileName: 'taxi_json.parquet',
      // });

      // Create views for raw and memory file manager after registering the files
      if (
        fileManagerType === 'raw' ||
        fileManagerType === 'memory' ||
        fileManagerType === 'opfs'
      ) {
        const useTable = fileManagerType === 'opfs';

        await dbm.query(generateViewQuery('taxi', ['taxi.parquet'], useTable));
        // await dbm.query(generateViewQuery('taxi_json', ['taxi_json.parquet'], useTable));
      }

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
