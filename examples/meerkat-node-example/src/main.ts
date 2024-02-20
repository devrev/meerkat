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
  const sql = `SELECT json_serialize_sql('SELECT CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 0 THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 1 AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END) > 0             THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 0 THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 1 THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 2 AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END)))         ELSE NULL     END FROM tbl1');`;
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
