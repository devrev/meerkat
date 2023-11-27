import { validateCacheByFiles } from './validate-cache-by-files';

describe('validateCacheByFiles', () => {
  const currentDate = new Date(2023, 11, 1, 12, 20);

  const files = [
    {
      date: new Date(2023, 11, 1, 12, 20),
      fileName: 'taxi1.parquet',
      cacheTime: 3600000,
    },
    {
      date: new Date(2023, 11, 1, 12, 40),
      fileName: 'taxi1.parquet',
      cacheTime: 3600000,
    },
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 13, 20),
      fileName: 'taxi1.parquet',
    },
    {
      cacheTime: 3600000,
      date: new Date(2023, 11, 1, 13, 40),
      fileName: 'taxi1.parquet',
    },
  ];

  jest
    .spyOn(global.Date, 'now')
    .mockImplementation(() => currentDate.getTime());

  it('should return an empty array for files when cacheTime is between the range', () => {
    // When the cache is requested at 12:20, the cacheTime is 3600000, which is 1 hour
    expect(validateCacheByFiles([files[0]])).toEqual([]);

    // When the request is 
    expect(validateCacheByFiles([files[1]])).toEqual([]);
  });
});
