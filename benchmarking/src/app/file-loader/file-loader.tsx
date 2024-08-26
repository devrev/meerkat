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
        'http://localhost:4200/data-sets/fhvhv_tripdata_2023-01.parquet',
        { responseType: 'arraybuffer' }
      );
      const fileBuffer = file.data;
      const sharedBuffer = new SharedArrayBuffer(fileBuffer.byteLength);
      const fileBufferView = new Uint8Array(sharedBuffer);
      fileBufferView.set(new Uint8Array(fileBuffer));

      const jsonFile = await axios.get(
        'http://localhost:4200/data-sets/taxi.json',
        { responseType: 'json' }
      );
      const TAXI_JSON_DATA = jsonFile.data;

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        buffer: sharedBuffer as any,
      });

      await fileManager.registerJSON({
        json: TAXI_JSON_DATA,
        tableName: 'taxijson',
        fileName: 'taxijson.parquet',
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
