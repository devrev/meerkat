import { useState } from 'react';
import TAXI_JSON_DATA from '../../../public/data-sets/taxi.json';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const NativeAppFileLoader = ({
  children,
}: {
  children: JSX.Element;
}) => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      const fileUrl =
        'http://localhost:4204/data-sets/fhvhv_tripdata_2023-01.parquet';

      await fileManager.registerFileBuffer({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        fileUrl: fileUrl,
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
