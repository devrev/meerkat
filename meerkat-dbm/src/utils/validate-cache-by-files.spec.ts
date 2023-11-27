import { validateCacheByFiles } from './validate-cache-by-files';


describe('validateCacheByFiles', () => {
  const date = new Date(2023, 11, 1, 12, 20);

  const files = [
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 12, 20),
      fileName: 'taxi1.parquet',
    },
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 12, 40),
      fileName: 'taxi1.parquet',
    },
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 13, 20),
      fileName: 'taxi1.parquet',
    },
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 11, 19),
      fileName: 'taxi1.parquet',
    },
  ];

  jest.spyOn(global, 'Date').mockImplementation(() => date);

  it('should return an empty array for files when cacheTime is between the range', () => {
    // When the is request is made at same time as the file was created
    expect(validateCacheByFiles([files[0]])).toEqual([]);

    // When the request is made between the cacheTime
    expect(validateCacheByFiles([files[1]])).toEqual([]);

    // When the request is made at the last minute of the cacheTime
    expect(validateCacheByFiles([files[2]])).toEqual([]);
  });

  it('should return stale files for files with expired cacheTime', () => {
    // When the is request is made after the cacheTime
    expect(validateCacheByFiles([files[3]])).toEqual(['taxi1.parquet']);
  });
});
