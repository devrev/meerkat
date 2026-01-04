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
    ('4', 'Emma Stone', 40, ARRAY['Photography', 'Painting']),
    ('5', 'Alex Johnson', 28, ARRAY['Running', 'Swimming', 'Cycling']);
`);
  });

  describe('useDotNotation: false (default)', () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('3');
    });

    it('Should construct the Equals SQL query and apply contains filter 1', async () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS person__activities, * FROM (select * from person) AS person) AS person WHERE list_has_all(person__activities, main.list_value('Hiking'))`
      );
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('3');
    });

    it('Should construct the Equals SQL query and apply apply other filters 2', async () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS person__activities, id AS person__id, * FROM (select * from person) AS person) AS person WHERE (list_has_all(person__activities, main.list_value('Running')) AND (person__id = '2'))`
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('SQL: ', sql);
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS person__activities, * FROM (select * from person) AS person) AS person WHERE (NOT list_has_all(person__activities, main.list_value('Running')))`
      );
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(2);
      expect(output[0].id).toBe('3');
      expect(output[1].id).toBe('4');
    });
    it('Should test array contains all specified elements (equals operator behavior)', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Running', 'Swimming'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('Array contains all elements SQL: ', sql);
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(2);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['1', '5']);
    });

    it('Should test array contains all specified elements in reverse order', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Swimming', 'Running'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('Array contains all elements in reverse order SQL: ', sql);
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(2);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['1', '5']);
    });

    it('Should test no match condition - searching for non-existent activity', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['NonExistentActivity'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('No match SQL: ', sql);
      const output: any = await duckdbExec(sql);
      // Should return no results
      expect(output).toHaveLength(0);
    });

    it('Should test complex array contains all with multiple elements', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Reading', 'Cooking', 'Running'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      console.info('Complex contains all SQL: ', sql);
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('2');
      expect(output[0].name).toBe('Jane Doe');
    });

    it('Should test notEquals - arrays that do NOT contain all specified elements', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'notEquals',
            values: ['Running', 'Swimming'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: false },
      });
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(3);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['2', '3', '4']);
    });
  });

  describe('useDotNotation: true', () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info('SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('3');
    });

    it('Should construct the Equals SQL query and apply contains filter 1', async () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS "person.activities", * FROM (select * from person) AS person) AS person WHERE list_has_all("person.activities", main.list_value('Hiking'))`
      );
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('3');
    });

    it('Should construct the Equals SQL query and apply apply other filters 2', async () => {
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS "person.activities", id AS "person.id", * FROM (select * from person) AS person) AS person WHERE (list_has_all("person.activities", main.list_value('Running')) AND ("person.id" = '2'))`
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
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info('SQL (dot notation): ', sql);
      expect(sql).toBe(
        `SELECT person.* FROM (SELECT activities AS "person.activities", * FROM (select * from person) AS person) AS person WHERE (NOT list_has_all("person.activities", main.list_value('Running')))`
      );
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(2);
      expect(output[0].id).toBe('3');
      expect(output[1].id).toBe('4');
    });

    it('Should test array contains all specified elements (equals operator behavior)', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Running', 'Swimming'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info('Array contains all elements SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(2);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['1', '5']);
    });

    it('Should test array contains all specified elements in reverse order', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Swimming', 'Running'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info(
        'Array contains all elements in reverse order SQL (dot notation): ',
        sql
      );
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(2);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['1', '5']);
    });

    it('Should test no match condition - searching for non-existent activity', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['NonExistentActivity'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info('No match SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      // Should return no results
      expect(output).toHaveLength(0);
    });

    it('Should test complex array contains all with multiple elements', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'equals',
            values: ['Reading', 'Cooking', 'Running'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      console.info('Complex contains all SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(1);
      expect(output[0].id).toBe('2');
      expect(output[0].name).toBe('Jane Doe');
    });

    it('Should test notEquals - arrays that do NOT contain all specified elements', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'person.activities',
            operator: 'notEquals',
            values: ['Running', 'Swimming'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [SCHEMA],
        options: { useDotNotation: true },
      });
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(3);
      const ids = output.map((row: any) => row.id);
      expect(ids).toEqual(['2', '3', '4']);
    });
  });
});
