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
  const sql = `SELECT json_serialize_sql('SELECT *
  FROM users
  WHERE country = "US" 
  OR country = "Germany"');`;
  const data = await nodeSQLToSerialization(sql);
  res.send({ message: data });
});

// SELECT json_deserialize_sql(json_serialize_sql('SELECT *  FROM users WHERE country IN ("US", "Germany", "Israel")'));

const port = process.env.PORT || 3333;
const server = app.listen(port, () => {
  console.log(`Listening at http://localhost:${port}/api`);
});
server.on('error', console.error);
