import { useState } from 'react';
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
      const taxiJsonResponse = await fetch('/data-sets/taxi.json');
      const taxiJsonData = await taxiJsonResponse.json();

      await fileManager.registerFileUrl?.({
        tableName: 'taxi',
        fileName: 'taxi',
        url: url,
      });

      await fileManager.registerJSON({
        json: taxiJsonData,
        tableName: 'taxi_json',
        fileName: 'taxi_json',
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
