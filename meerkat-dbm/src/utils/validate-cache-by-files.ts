import { FileData } from '../file-manager/file-manager-type';

export const validateCacheByFiles = (files: FileData[]): string[] => {
  const staleFiles: string[] = [];

  for (const file of files) {
    const { fileName, date, cacheTime } = file;

    if (cacheTime) {
      const currentTime = Date.now();

      const timeStamp = date.getTime();

      const fileTime = timeStamp + cacheTime;

      if (currentTime >= fileTime) {
        staleFiles.push(fileName);
      }
    }
  }

  return staleFiles;
};
