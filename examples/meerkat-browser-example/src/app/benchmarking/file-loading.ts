import { AsyncDuckDB } from '@kunal-mohta/duckdb-wasm';
import axios from 'axios';

export const fileLoadingBenchmark = async (db: AsyncDuckDB) => {
  let file1: any = await axios({
    method: 'get',
    url: 'http://localhost:3333/api/file-1',
    responseType: 'arraybuffer',
  });
  const start1 = performance.now();
  await db.registerFileBuffer('mixjs.parquet', new Uint8Array(file1.data));
  const end1 = performance.now();

  const con = await db.connect();

  // await con.query(`SET home_directory=''`);
  // await con.query(`INSTALL json`);
  // await con.query(`LOAD json`);

  // const start2 = performance.now();
  // const res = await con.query(
  //   `SELECT COUNT(id), composite.address FROM 'mixjs.parquet' GROUP BY composite.address`
  // );
  // const end2 = performance.now();
  // console.info('struct', end2 - start2);

  // console.log('Res: ', res.toString());

  // const start3 = performance.now();
  // const res = await con.query(
  //   `SELECT COUNT(id), mp['address'] FROM 'mixjs.parquet' GROUP BY mp['address']`
  // );
  // const end3 = performance.now();
  // console.info('map', end3 - start3);

  const start3 = performance.now();
  const res = await con.query(`
    SELECT json_extract(js, '$.address') FROM 'mixjs.parquet';
  `);
  console.log(res.toString());
  const end3 = performance.now();
  console.info('js', end3 - start3);

  // console.log('Res: ', res.toString());

  file1 = null;
};
