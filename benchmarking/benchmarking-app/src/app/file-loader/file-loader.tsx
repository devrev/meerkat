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

      await fileManager.registerJSON({
        json: TAXI_JSON_DATA,
        tableName: 'taxijson',
        fileName: 'taxijson.parquet',
      });

      //Find all iframe and add fileBufferView & TAXI_JSON_DATA to the window
      const iframes = document.querySelectorAll('iframe');
      iframes.forEach((iframe) => {
        const win = iframe.contentWindow;
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        //@ts-ignore
        win.fileBufferView = fileBufferView;
        console.log('fileBufferView', fileBufferView);
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        //@ts-ignore
        win.TAXI_JSON_DATA = TAXI_JSON_DATA;
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
