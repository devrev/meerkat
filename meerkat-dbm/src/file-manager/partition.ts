import { FileData } from '../types';

export const getFilesByPartition = (
  files: FileData[],
  partitions?: string[]
) => {
  // If partition is not defined, return all files
  if (!partitions?.length) return files;

  // Filter files by partition
  const filteredFiles = files.filter((file) => {
    // If file has no partitions, skip it
    if (!file.partitions?.length) return false;

    const filePartitions = file.partitions;

    // Check if any partition in the file matches any partition in the input
    return partitions.some((requestPartition) => {
      return filePartitions.some((filePartition) => {
        return filePartition.includes(requestPartition);
      });
    });
  });

  return filteredFiles;
};
