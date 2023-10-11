import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const SCHEMA = {
  name: 'person',
  sql: 'select * from person',
  measures: [
    {
      name: 'total_rows',
      sql: 'COUNT(*)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      name: 'name',
      sql: 'name',
      type: 'string',
    },
    {
      name: 'age',
      sql: 'age',
      type: 'number',
    },
    {
      name: 'activities',
      sql: 'activities',
      type: 'string_array',
    },
  ],
};
describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(`
    CREATE TABLE person (
        id VARCHAR,
        name VARCHAR,
        age INTEGER,
        activities VARCHAR[]
    );
    `);
    //Insert test data
    await duckdbExec(`
    INSERT INTO person (id, name, age, activities) 
    VALUES 
    ('1', 'John Doe', 30, ARRAY['Running', 'Swimming']),
    ('2', 'Jane Doe', 25, ARRAY['Reading', 'Cooking', 'Running']),
    ('3', 'Sam Smith', 35, ARRAY['Cycling', 'Hiking']),
    ('4', 'Emma Stone', 40, ARRAY['Photography', 'Painting']);
`);
  });

  it('Should construct the SQL query and apply contains filter', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'person.activities',
          operator: 'equals',
          values: ['Hiking'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].id).toBe('3');
  });

  it('Should construct the Equals SQL query and apply contains filter', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'person.activities',
          operator: 'equals',
          values: ['Hiking'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    expect(sql).toBe(
      `SELECT person.* FROM (select * from person) AS person WHERE ('Hiking' = ANY(SELECT unnest(person.activities)))`
    );
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].id).toBe('3');
  });

  it('Should construct the Equals SQL query and apply apply other filters', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          and: [
            {
              member: 'person.activities',
              operator: 'equals',
              values: ['Running'],
            },
            {
              member: 'person.id',
              operator: 'equals',
              values: ['2'],
            },
          ],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    expect(sql).toBe(
      `SELECT person.* FROM (select * from person) AS person WHERE (('Running' = ANY(SELECT unnest(person.activities))) AND (person.id = '2'))`
    );
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].id).toBe('2');
  });

  it('Should construct the Not Equals SQL query and apply contains filter', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'person.activities',
          operator: 'notEquals',
          values: ['Running'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    console.info('SQL: ', sql);
    expect(sql).toBe(
      `SELECT person.* FROM (select * from person) AS person WHERE (NOT ('Running' = ANY(SELECT unnest(person.activities))))`
    );
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output[0].id).toBe('3');
    expect(output[1].id).toBe('4');
  });
});
