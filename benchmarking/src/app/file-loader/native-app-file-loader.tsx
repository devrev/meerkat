import { useState } from 'react';
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

      await fileManager.registerFileUrl({
        tableName: 'taxi',
        fileName: 'taxi.parquet',
        fileUrl: fileUrl,
      });

      setIsFileLoader(true);
    })();
  }, []);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  return <>{children}</>;
};
