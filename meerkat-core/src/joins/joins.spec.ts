import { getUsedTableSchema } from '../get-used-table-schema/get-used-table-schema';
import { Query, TableSchema } from '../types/cube-types';
import {
  checkLoopInJoinPath,
  createDirectedGraph,
  generateSqlQuery,
  getCombinedTableSchema,
} from './joins';

describe('Table schema functions', () => {
  it('should create a directed graph from the table schema', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [{ sql: 'table1.id = table2.id' }],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    });
  });

  it('should ignore a directed graph edge from the table schema if not present in query map', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [
          { sql: 'table1.id = table2.id' },
          { sql: 'table1.id = table4.id' },
        ],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
    });
  });

  it('should correctly generate a SQL query from the provided join path, table schema SQL map, and directed graph', () => {
    const joinPaths = [
      [
        { left: 'table1', right: 'table2', on: 'id' },
        { left: 'table2', right: 'table3', on: 'id' },
      ],
    ];
    const directedGraph = {
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    };
    const tableSchemaSqlMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const sqlQuery = generateSqlQuery(
      joinPaths,
      tableSchemaSqlMap,
      directedGraph
    );

    expect(sqlQuery).toBe(
      'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id LEFT JOIN (select * from table3) AS table3  ON table2.id = table3.id'
    );
  });

  describe('checkLoopInJoinPath', () => {
    it('should return false if there is no loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
    it('should return true if there is a loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
          { left: 'table3', right: 'table1', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(true);
    });
    it('should return false for single node', () => {
      const joinPath = [[{ left: 'table1' }, { left: 'table1' }]];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
  });

  describe('getCombinedTableSchema', () => {
    it('should return single table schema when only one table is provided', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [{ name: 'measure1' }],
          dimensions: [{ name: 'dimension1' }],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [],
      };

      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema[0]);
    });

    it('should combine multiple table schemas correctly', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [{ name: 'measure1' }],
          dimensions: [{ name: 'dimension1' }],
          joins: [{ sql: 'table1.id = table2.id' }],
        },
        {
          name: 'table2',
          sql: 'select * from table2',
          measures: [{ name: 'measure2' }],
          dimensions: [{ name: 'dimension2' }],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [[{ left: 'table1', right: 'table2', on: 'id' }]],
      };

      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id',
        measures: [{ name: 'measure1' }, { name: 'measure2' }],
        dimensions: [{ name: 'dimension1' }, { name: 'dimension2' }],
        joins: [],
      });
    });

    it('should throw error when loop is detected in join paths', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [],
          dimensions: [],
          joins: [{ sql: 'table1.id = table2.id' }],
        },
        {
          name: 'table2',
          sql: 'select * from table2',
          measures: [],
          dimensions: [],
          joins: [{ sql: 'table2.id = table1.id' }],
        },
      ];
      const cubeQuery = {
        joinPaths: [
          [
            { left: 'table1', right: 'table2', on: 'id' },
            { left: 'table2', right: 'table1', on: 'id' },
          ],
        ],
      };

      await expect(
        getCombinedTableSchema(tableSchema, cubeQuery)
      ).rejects.toThrow(/A loop was detected in the joins/);
    });

    it('should handle empty measures and dimensions', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [],
          dimensions: [],
          joins: [{ sql: 'table1.id = table2.id' }],
        },
        {
          name: 'table2',
          sql: 'select * from table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [[{ left: 'table1', right: 'table2', on: 'id' }]],
      };

      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id',
        measures: [],
        dimensions: [],
        joins: [],
      });
    });
    it('should filter table schema based on filters, measures, and dimensions', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'table1',
          sql: 'SELECT * FROM table1',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table2',
          sql: 'SELECT * FROM table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table3',
          sql: 'SELECT * FROM table3',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ];

      const cubeQuery: Query = {
        measures: ['table1.measure1'],
        dimensions: ['table2.dimension1'],
        filters: [
          {
            member: 'table3.dimension2',
            operator: 'equals',
            values: ['value'],
          },
        ],
      };

      const result = getUsedTableSchema(tableSchema, cubeQuery);

      expect(result).toEqual([
        {
          name: 'table1',
          sql: 'SELECT * FROM table1',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table2',
          sql: 'SELECT * FROM table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table3',
          sql: 'SELECT * FROM table3',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ]);
    });
    it('should filter table schema based on filters, measures, dimensions, order, and joinPaths', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'table1',
          sql: 'SELECT * FROM table1',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table2',
          sql: 'SELECT * FROM table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table3',
          sql: 'SELECT * FROM table3',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table4',
          sql: 'SELECT * FROM table4',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ];

      const cubeQuery: Query = {
        measures: ['table1.measure1'],
        dimensions: ['table2.dimension1'],
        filters: [
          {
            member: 'table3.dimension2',
            operator: 'equals',
            values: ['value'],
          },
        ],
        order: {
          'table4.dimension3': 'asc',
        },
        joinPaths: [
          [
            {
              left: 'table1.dimension1',
              right: 'table2.dimension1',
              on: 'id',
            },
          ],
        ],
      };

      const result = getUsedTableSchema(tableSchema, cubeQuery);

      expect(result).toEqual([
        {
          name: 'table1',
          sql: 'SELECT * FROM table1',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table2',
          sql: 'SELECT * FROM table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table3',
          sql: 'SELECT * FROM table3',
          measures: [],
          dimensions: [],
          joins: [],
        },
        {
          name: 'table4',
          sql: 'SELECT * FROM table4',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ]);
    });
  });
});
