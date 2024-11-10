import { convertUint8ArrayToSharedArrayBuffer } from '@devrev/meerkat-dbm';
import axios from 'axios';
import { useState } from 'react';
import TAXI_JSON_DATA from '../../../public/data-sets/taxi.json';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

type BufferType = 'sharedArrayBuffer' | 'uint8Array';

export const FileLoader = ({
  children,
  bufferType = 'uint8Array',
}: {
  children: JSX.Element;
  bufferType: BufferType;
}) => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      const file = await axios.get(
        'http://localhost:4201/data-sets/fhvhv_tripdata_2023-01.parquet',
        { responseType: 'arraybuffer' }
      );
      const fileBuffer = file.data;

      if (bufferType === 'sharedArrayBuffer') {
        const sharedBuffer = convertUint8ArrayToSharedArrayBuffer(fileBuffer);

        await fileManager.registerFileBuffer({
          tableName: 'taxi',
          fileName: 'taxi.parquet',
          buffer: sharedBuffer as any,
        });
      } else {
        const fileBufferView = new Uint8Array(fileBuffer);

        await fileManager.registerFileBuffer({
          tableName: 'taxi',
          fileName: 'taxi.parquet',
          buffer: fileBufferView,
        });
      }

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
