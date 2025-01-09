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

      await fileManager?.registerFileUrl({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        url: url,
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
