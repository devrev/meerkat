/**
 * This is not a production server yet!
 * This is only a minimal backend to get started.
 */

import { nodeSQLToSerialization } from '@devrev/meerkat-node';
import express from 'express';
import * as path from 'path';

const app = express();

app.use('/assets', express.static(path.join(__dirname, 'assets')));

app.get('/api', async (req, res) => {
  const sql = `SELECT json_serialize_sql('SELECT * from dim_issue where owner="nikhil" and (age > 10 or age < 100) ');`;
  const data = await nodeSQLToSerialization(sql);
  res.send({ message: data });
});

const port = process.env.PORT || 3333;
const server = app.listen(port, () => {
  console.log(`Listening at http://localhost:${port}/api`);
});
server.on('error', console.error);
