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
        'http://localhost:4200/assets/data-sets/fhvhv_tripdata_2020-06.parquet',
        { responseType: 'arraybuffer' }
      );
      const fileBuffer = file.data;
      let fileBufferView = new Uint8Array(fileBuffer);

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        buffer: fileBufferView,
      });
      fileBufferView = new Uint8Array([1]);

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
