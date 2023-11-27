import { DEFAULT_CACHE_TIME } from '../constants';
import { FileData } from '../file-manager/file-manager-type';

/**
 * Validates files based on their cacheTime.
 * @returns An array of file names that are stale based on their cacheTime.
 */
export const validateCacheByFiles = (files: FileData[]): string[] => {
  const invalidFiles: string[] = [];

  for (const file of files) {
    const { fileName, date, cacheTime = DEFAULT_CACHE_TIME } = file;

    // Get the current time
    const currentTime = new Date().getTime();

    // Get the timestamp of the file and add the cacheTime to it
    const timeStamp = date.getTime();

    const fileTime = timeStamp + cacheTime;

    // Check if the file has expired based on the cacheTime and add it to the array of invalid files
    if (currentTime >= fileTime) {
      invalidFiles.push(fileName);
    }
  }

  // Return the array of invalid file names
  return invalidFiles;
};
