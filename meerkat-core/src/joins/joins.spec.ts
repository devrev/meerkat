import {
  checkLoopInGraph,
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

  it('should correctly identify if a loop exists in the graph', () => {
    const graph = {
      table1: {
        table2: {
          field1: 'table2.field3 = table1.field4',
        },
        table3: {
          field2: 'table3.field5 = table1.field2',
        },
      },
      table2: {
        table3: {
          field3: 'table3.field4 = table2.field3',
        },
      },
      table3: {
        table1: {
          field5: 'table1.field1 = table3.field2',
        },
      },
    };
    const hasLoop = checkLoopInGraph(graph);

    expect(hasLoop).toBe(true);
  });

  it('should correctly generate a SQL query from the provided join path, table schema SQL map, and directed graph', () => {
    const joinPath = [
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
      joinPath,
      tableSchemaSqlMap,
      directedGraph
    );

    expect(sqlQuery).toBe(
      'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id LEFT JOIN (select * from table3) AS table3  ON table2.id = table3.id'
    );
  });

  it('should throw an error when a cycle exists in checkLoopInGraph', () => {
    const graph = {
      node1: { node2: { id: 'node1.id = node2.id' } },
      node2: { node3: { id: 'node2.id = node3.id ' } },
      node3: { node1: { id: 'node3.id = node1.id' } },
    };
    const output = checkLoopInGraph(graph);
    expect(output).toBe(true);
  });

  it('checkLoopInGraph should return false for disconnected graph', () => {
    const graph = {
      node1: { node2: { id: 'node1.id = node2.id ' } },
      node3: { node4: { id: 'node3.id = node4.id ' } },
    };
    expect(checkLoopInGraph(graph)).toBe(false);
  });
});
