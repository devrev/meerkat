import {
  TableSchema,
  convertCubeStringToTableSchema,
} from '@devrev/meerkat-core';
import axios from 'axios';
import { useState } from 'react';
import { useClassicEffect } from '../../../../hooks/use-classic-effect';
import { DEFAULT_DATA_SOURCES } from '../../data-sources/constants/data-sources';

export const useListDataSources = () => {
  const [dataSources, setDataSources] = useState<TableSchema[]>([]);

  useClassicEffect(() => {
    (async () => {
      // Fetch data sources from the API
      const promiseArray = [];
      for (let i = 0; i < DEFAULT_DATA_SOURCES.files.length; i++) {
        const fileName = DEFAULT_DATA_SOURCES.files[i];
        const url = `/files/${fileName}.txt`;
        const axiosPromise = axios.get(url, { responseType: 'arraybuffer' });
        promiseArray.push(axiosPromise);
      }

      const filesRes = await Promise.all(promiseArray);

      const files = filesRes
        .map((file) => {
          if (!file.data) return null;
          const fileData = new TextDecoder().decode(file.data);
          const dataSource = convertCubeStringToTableSchema(fileData);
          if (!dataSource) return null;
          return dataSource;
        })
        .filter(Boolean) as TableSchema[];

      if (!files) return;

      setDataSources(files);
      console.info(files);
    })();
  }, []);

  return { dataSources };
};
