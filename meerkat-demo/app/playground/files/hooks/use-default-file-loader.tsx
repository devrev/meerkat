import axios from 'axios';
import { useState } from 'react';
import { useDBM } from '../../../../hooks/dbm-context';
import { useClassicEffect } from '../../../../hooks/use-classic-effect';
import { DEFAULT_FILES } from '../constants/files';

export const useDefaultFileLoader = () => {
  const { fileManager } = useDBM();
  const [isFileLoader, setIsFileLoader] = useState<boolean>(false);

  useClassicEffect(() => {
    (async () => {
      const totalFiles = await fileManager.getTotalFilesCount();
      if (totalFiles > 0) {
        setIsFileLoader(true);
        return;
      }
      const promiseArray = [];
      for (let i = 0; i < DEFAULT_FILES.files.length; i++) {
        // Create relative URL for file
        const fileName = DEFAULT_FILES.files[i];
        const url = `/files/${fileName}.parquet`;
        const axiosPromise = axios.get(url, { responseType: 'arraybuffer' });
        promiseArray.push(axiosPromise);
      }
      const fileDataArray = await Promise.all(promiseArray);
      const promiseRegisterArray = [];
      // Register file buffer in DBM
      for (let i = 0; i < fileDataArray.length; i++) {
        const fileBuffer = fileDataArray[i].data;
        const tableName = DEFAULT_FILES.files[i];
        const fileBufferView = new Uint8Array(fileBuffer);
        const registerFilePromise = fileManager.registerFileBuffer({
          tableName: tableName,
          fileName: `${DEFAULT_FILES.files[i]}.parquet`,
          buffer: fileBufferView,
        });
        promiseRegisterArray.push(registerFilePromise);
      }
      await Promise.all(promiseRegisterArray);
      setIsFileLoader(true);
    })();
  }, [fileManager, setIsFileLoader]);

  return isFileLoader;
};
