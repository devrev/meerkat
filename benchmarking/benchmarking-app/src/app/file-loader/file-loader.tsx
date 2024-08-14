import axios from 'axios';
import { useState } from 'react';
import TAXI_JSON_DATA from '../../assets/data-sets/taxi.json';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const FileLoader = ({ children }: { children: JSX.Element }) => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);
  useClassicEffect(() => {
    (async () => {
      const params = new URLSearchParams(window.location.search);
      const key = params.get('key') ?? '1';

      const file = await axios.get(
        `http://localhost:4200/assets/data-sets/fhvhv_tripdata_2023-0${key}.parquet`,
        { responseType: 'arraybuffer' }
      );
      const fileBuffer = file.data;
      const fileBufferView = new Uint8Array(fileBuffer);

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        buffer: fileBufferView,
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
