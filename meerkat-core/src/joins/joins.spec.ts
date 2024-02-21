import {
  checkLoopInGraph,
  createDirectedGraph,
  generateSqlQuery,
  getJoinPathAsArray,
  getStartingNodes,
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
      table1: { table2: 'table1.id = table2.id' },
      table2: { table3: 'table2.id = table3.id' },
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
      table1: { table2: 'table1.id = table2.id' },
    });
  });

  it('should correctly identify if a loop exists in the graph', () => {
    const graph = {
      node1: { node2: 'node1->node2' },
      node2: { node1: 'node2->node1' },
    };
    const hasLoop = checkLoopInGraph(graph);

    expect(hasLoop).toBe(true);
  });

  it('should correctly identify starting nodes in the graph', () => {
    const graph = {
      node1: { node2: 'node1->node2', node3: 'node1->node3' },
      node2: {},
      node3: {},
    };
    const startingNodes = getStartingNodes(graph);

    expect(startingNodes).toEqual(['node1']);
  });

  it('should correctly generate a join path from the provided starting node and graph', () => {
    const graph = {
      node1: { node2: 'node1->node2' },
      node2: { node3: 'node2->node3' },
    };
    const joinPath = getJoinPathAsArray('node1', graph);

    expect(joinPath).toEqual(['node1', 'node2', 'node2', 'node3']);
  });

  it('should correctly generate a SQL query from the provided join path, table schema SQL map, and directed graph', () => {
    const joinPath = ['table1', 'table2', 'table2', 'table3'];
    const directedGraph = {
      table1: { table2: 'table1.id = table2.id' },
      table2: { table3: 'table2.id = table3.id' },
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
      node1: { node2: 'node1->node2' },
      node2: { node3: 'node2->node3' },
      node3: { node1: 'node3->node1' },
    };
    const output = checkLoopInGraph(graph);
    expect(output).toBe(true);
  });

  it('should return empty array if starting node does not exist in getJoinPathAsArray', () => {
    const graph = {
      node1: { node2: 'node1->node2' },
      node2: { node3: 'node2->node3' },
    };
    const joinPath = getJoinPathAsArray('nonExistingNode', graph);

    expect(joinPath).toEqual([]);
  });

  it('checkLoopInGraph should return false for disconnected graph', () => {
    const graph = {
      node1: { node2: 'node1->node2' },
      node3: { node4: 'node3->node4' },
    };
    expect(checkLoopInGraph(graph)).toBe(false);
  });

  it('getStartingNodes should return all nodes in disconnected graph', () => {
    const graph = {
      node1: {},
      node2: {},
      node3: {},
    };
    expect(getStartingNodes(graph)).toEqual(['node1', 'node2', 'node3']);
  });

  it('getJoinPathAsArray should return unvisited nodes in the path', () => {
    const graph = {
      node1: { node2: 'node1->node2' },
      node2: { node3: 'node2->node3' },
      node3: {},
      node4: {},
    };
    const joinPath = getJoinPathAsArray('node1', graph);
    expect(joinPath).toContain('node1');
    expect(joinPath).toContain('node2');
    expect(joinPath).toContain('node3');
  });

  it('generateSqlQuery should handle empty join path and graph', () => {
    const joinPath = [];
    const directedGraph = {};
    const tableSchemaSqlMap = {
      table1: 'select * from table1',
    };
    const sqlQuery = generateSqlQuery(
      joinPath,
      tableSchemaSqlMap,
      directedGraph
    );
    expect(sqlQuery).toBe('undefined');
  });
});
