/**
 * This is not a production server yet!
 * This is only a minimal backend to get started.
 */

import { cubeQueryToSQL, nodeSQLToSerialization } from '@devrev/meerkat-node';
import express from 'express';
import fs from 'fs';
import * as path from 'path';

const app = express();

app.use(express.json());

app.use((req, res, next) => {
  // Get ORIGIN from request
  const origin = req.get('origin');
  if (!origin) {
    return next();
  }
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

app.use('/assets', express.static(path.join(__dirname, 'assets')));

app.get('/api/file-1', (req, res) => {
  const filePath = path.join(
    '/Users/nikhiltalwar/Workspace/devrev/meerkat/examples/meerkat-node-example/data/fhvhv_tripdata_2023-01.parquet'
  );

  const fileStream = fs.createReadStream(filePath);
  fileStream.pipe(res);
});

app.get('/api/file-2', (req, res) => {
  const filePath = path.join(
    '/Users/nikhiltalwar/Workspace/devrev/meerkat/examples/meerkat-node-example/data/fhvhv_tripdata_2023-02.parquet'
  );

  const fileStream = fs.createReadStream(filePath);
  fileStream.pipe(res);
});

app.get('/api/file-3', (req, res) => {
  const filePath = path.join(
    '/Users/nikhiltalwar/Workspace/devrev/meerkat/examples/meerkat-node-example/data/fhvhv_tripdata_2023-03.parquet'
  );

  const fileStream = fs.createReadStream(filePath);
  fileStream.pipe(res);
});

app.get('/api/file-4', (req, res) => {
  const filePath = path.join(
    '/Users/nikhiltalwar/Workspace/devrev/meerkat/examples/meerkat-node-example/data/fhvhv_tripdata_2023-04.parquet'
  );

  const fileStream = fs.createReadStream(filePath);
  fileStream.pipe(res);
});

app.get('/api', async (req, res) => {
  const sql = `SELECT json_serialize_sql('SELECT * FROM T1 ORDER BY VAL ASC, BEL ASC LIMIT 10');`;
  const data = await nodeSQLToSerialization(sql);
  res.json({ message: data });
});

app.post('/api-v1', async (req, res) => {
  const { cube, table_schema } = req.body;
  // const query = await cubeQueryToSQL(sql, cube);

  const data = await cubeQueryToSQL(cube, table_schema);

  res.json({ data });
});

// SELECT json_deserialize_sql(json_serialize_sql('SELECT *  FROM users WHERE country IN ("US", "Germany", "Israel")'));

const port = process.env.PORT || 3333;
const server = app.listen(port, () => {
  console.log(`Listening at http://localhost:${port}/api`);
});
server.on('error', console.error);
