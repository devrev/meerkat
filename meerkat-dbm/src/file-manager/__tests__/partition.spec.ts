import { FileData } from '../../types';
import { getFilesByPartition } from '../partition';

describe('getFilesByPartition', () => {
  const files: FileData[] = [
    { fileName: 'file1.txt', partitions: ['customer_oid=dev-0'] },
    { fileName: 'file2.txt', partitions: ['customer_oid=dev-0/month=09'] },
    { fileName: 'file3.txt', partitions: ['customer_oid=dev-1'] },
    { fileName: 'file4.txt' },
    { fileName: 'file6.txt', partitions: ['customer_oid=dev-0/month=10'] },
  ];

  it('should return all files when partition is not defined', () => {
    const result = getFilesByPartition(files);
    expect(result).toEqual(files);
  });

  it('should filter files by partition when partition is defined', () => {
    const result = getFilesByPartition(files, ['customer_oid=dev-0']);
    expect(result).toEqual([
      { fileName: 'file1.txt', partitions: ['customer_oid=dev-0'] },
      { fileName: 'file2.txt', partitions: ['customer_oid=dev-0/month=09'] },
      { fileName: 'file6.txt', partitions: ['customer_oid=dev-0/month=10'] },
    ]);
  });

  it('should filter files by multiple partitions of customer_oid', () => {
    const result = getFilesByPartition(files, [
      'customer_oid=dev-0',
      'customer_oid=dev-1',
    ]);

    expect(result).toEqual([
      { fileName: 'file1.txt', partitions: ['customer_oid=dev-0'] },
      { fileName: 'file2.txt', partitions: ['customer_oid=dev-0/month=09'] },
      { fileName: 'file3.txt', partitions: ['customer_oid=dev-1'] },
      { fileName: 'file6.txt', partitions: ['customer_oid=dev-0/month=10'] },
    ]);
  });

  it('should filter files by a specific combination of partitions ', () => {
    const result = getFilesByPartition(files, ['customer_oid=dev-0/month=09']);

    expect(result).toEqual([
      { fileName: 'file2.txt', partitions: ['customer_oid=dev-0/month=09'] },
    ]);
  });

  it('should filter files by multiple specified partitions', () => {
    const result = getFilesByPartition(files, [
      'customer_oid=dev-0/month=09',
      'customer_oid=dev-0/month=10',
    ]);

    expect(result).toEqual([
      { fileName: 'file2.txt', partitions: ['customer_oid=dev-0/month=09'] },
      { fileName: 'file6.txt', partitions: ['customer_oid=dev-0/month=10'] },
    ]);
  });

  it('should return an empty array when no matching files are found', () => {
    const result = getFilesByPartition(files, ['customer_oid=dev-3']);
    expect(result).toEqual([]);
  });
});
