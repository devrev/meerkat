import * as duckdb from 'duckdb';

const query = `SELECT json_serialize_sql('SELECT 2');`;

export function meerkatNode(): string {
  const db = new duckdb.Database(':memory:');
  db.all(query, (err, res) => {
    console.log(res);
  });
  return 'meerkat-node';
}
