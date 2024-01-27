import { FileData } from '../types/db-schema';

export const getFilesByPartition = (
  files: FileData[],
  partitions?: string[]
) => {
  // If partition is not defined, return all files
  if (!partitions?.length) return files;

  // Filter files by partition
  const filteredFiles = files.filter((file) => {
    if (!file.partitionKey) return false;

    return partitions.some((partition) =>
      file.partitionKey?.includes(partition)
    );
  });

  return filteredFiles;
};
