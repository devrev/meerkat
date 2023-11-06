import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import axios from 'axios';

export const fileLoadingBenchmark = async (db: AsyncDuckDB) => {
  let file1: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-1',
    responseType: 'arraybuffer',
  });
  const start1 = performance.now();
  await db.registerFileBuffer('taxi-1.parquet', new Uint8Array(file1.data));
  const end1 = performance.now();

  file1 = null;

  console.info('file 1', end1 - start1);

  let file2: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-2',
    responseType: 'arraybuffer',
  });
  const start2 = performance.now();
  await db.registerFileBuffer('taxi-2.parquet', new Uint8Array(file2.data));
  const end2 = performance.now();

  file2 = null;

  console.info('file 2', end2 - start2);

  let file3: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-3',
    responseType: 'arraybuffer',
  });
  const start3 = performance.now();
  await db.registerFileBuffer('taxi-3.parquet', new Uint8Array(file3.data));
  const end3 = performance.now();

  file3 = null;

  console.info('file 3', end3 - start3);

  let file4: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  const start4 = performance.now();
  await db.registerFileBuffer('taxi-4.parquet', new Uint8Array(file4.data));
  const end4 = performance.now();

  file4 = null;

  console.info('file 4', end4 - start4);

  const file5: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  await db.registerFileBuffer('taxi-5.parquet', new Uint8Array(file5.data));

  file4 = null;

  console.info('file 5', end4 - start4);

  const file6: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  await db.registerFileBuffer('taxi-6.parquet', new Uint8Array(file6.data));

  file4 = null;

  console.info('file 6', end4 - start4);

  const file7: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  await db.registerFileBuffer('taxi-7.parquet', new Uint8Array(file7.data));

  file4 = null;

  console.info('file 7', end4 - start4);

  const file8: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  await db.registerFileBuffer('taxi-8.parquet', new Uint8Array(file8.data));

  file4 = null;

  console.info('file 8', end4 - start4);

  const file9: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-4',
    responseType: 'arraybuffer',
  });

  await db.registerFileBuffer('taxi-9.parquet', new Uint8Array(file9.data));

  file4 = null;

  console.info('file 9', end4 - start4);
};
