import { useState } from 'react';
import TAXI_JSON_DATA from '../../assets/data-sets/taxi.json';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const JsonLoader = ({ children }: { children: JSX.Element }) => {
  const { fileManager } = useDBM();
  const [isLoading, setIsLoading] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      await fileManager.registerJSON({
        json: TAXI_JSON_DATA,
        tableName: 'taxi',
        fileName: 'taxi.json',
      });

      setIsLoading(true);
    })();
  }, []);

  if (!isLoading) {
    return <div id="loader">Registering JSON</div>;
  }

  return <>{children}</>;
};
