import axios from 'axios';
import { useState } from 'react';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const FileLoader = ({ children }: { children: JSX.Element }) => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);
  useClassicEffect(() => {
    (async () => {
      const file = await axios.get(
        'http://localhost:4200/assets/data-sets/fhvhv_tripdata_2023-01.parquet',
        { responseType: 'arraybuffer' }
      );
      const fileBuffer = file.data;
      const fileBufferView = new Uint8Array(fileBuffer);

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        buffer: fileBufferView,
      });

      // await fileManager.registerFileBuffer({
      //   tableName: 'taxi',
      //   fileName: 'taxi123.parquet',
      //   buffer: fileBufferView,
      // });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
