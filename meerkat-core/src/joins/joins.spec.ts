import {
  checkLoopInJoinPath,
  createDirectedGraph,
  generateSqlQuery,
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
    })
    it('should return true if there is a loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
          { left: 'table3', right: 'table1', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(true);
    })
    it('should return false for single node', () => {
      const joinPath = [
        [
          { left: 'table1', },
          { left: 'table1' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    })
  })
});
