/**
 * This is not a production server yet!
 * This is only a minimal backend to get started.
 */

import { cubeQueryToSQL, nodeSQLToSerialization } from '@devrev/meerkat-node';
import express from 'express';
import * as path from 'path';

const app = express();

app.use(express.json());

app.use('/assets', express.static(path.join(__dirname, 'assets')));

app.get('/api', async (req, res) => {
  const sql = `SELECT json_serialize_sql('SELECT *
  FROM posts
  WHERE time >= 1 AND time <= 2');`;
  const data = await nodeSQLToSerialization(sql);
  res.json({ message: data });
});

app.post('/api-v1', async (req, res) => {
  const { cube, sql } = req.body;
  // const query = await cubeQueryToSQL(sql, cube);

  const data = await cubeQueryToSQL(cube);

  res.json({ data });
});

// SELECT json_deserialize_sql(json_serialize_sql('SELECT *  FROM users WHERE country IN ("US", "Germany", "Israel")'));

const port = process.env.PORT || 3333;
const server = app.listen(port, () => {
  console.log(`Listening at http://localhost:${port}/api`);
});
server.on('error', console.error);
