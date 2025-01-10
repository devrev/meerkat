import { useState } from 'react';
import TAXI_JSON_DATA from '../../../public/data-sets/taxi.json';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { TAXI_FILE_URL } from './constants';

export const NativeAppFileLoader = ({
  children,
}: {
  children: JSX.Element;
}) => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      const url = TAXI_FILE_URL;

      await fileManager.registerFileUrl?.({
        tableName: 'taxi',
        fileName: 'taxi',
        url: url,
      });

      await fileManager.registerJSON({
        json: TAXI_JSON_DATA,
        tableName: 'taxijson',
        fileName: 'taxijson',
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
