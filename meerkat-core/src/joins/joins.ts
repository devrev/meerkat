import { JoinEdge, Query, TableSchema } from '../types/cube-types';

export type Graph = {
  [key: string]: { [key: string]: { [key: string]: string } };
};

export function generateSqlQuery(
  path: JoinEdge[][],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph
): string {
  if (path.length === 0) {
    throw new Error(
      'Invalid path, multiple data sources are present without a join path.'
    );
  }

  const startingNode = path[0][0].left;
  let query = `${tableSchemaSqlMap[startingNode]}`;

  const visitedNodes = new Map();

  for (let i = 0; i < path.length; i++) {
    if (path[i][0].left !== startingNode) {
      throw new Error(
        'Invalid path, starting node is not the same for all paths.'
      );
    }
    for (let j = 0; j < path[i].length; j++) {
      const currentEdge = path[i][j];
      const visitedFrom = visitedNodes.get(currentEdge.right);

      // If node is already visited from the same edge, continue to next iteration
      if (visitedFrom && visitedFrom.left === currentEdge.left) {
        continue;
      }
      // If node is already visited from a different edge, throw ambiguity error
      if (visitedFrom) {
        throw new Error(
          `Path ambiguity, node ${currentEdge.right} visited from different sources`
        );
      }

      // If visitedFrom is undefined, this is the first visit to the node
      visitedNodes.set(currentEdge.right, currentEdge);

      query += ` LEFT JOIN (${tableSchemaSqlMap[currentEdge.right]}) AS ${
        currentEdge.right
      }  ON ${
        directedGraph[currentEdge.left][currentEdge.right][currentEdge.on]
      }`;
    }
  }

  return query;
}

export const createDirectedGraph = (
  tableSchema: TableSchema[],
  tableSchemaSqlMap: { [key: string]: string }
) => {
  const directedGraph: {
    [key: string]: { [key: string]: { [key: string]: string } };
  } = {};

  function addEdge(
    table1: string,
    table2: string,
    joinOn: string,
    joinCondition: string
  ) {
    if (
      table1 === table2 ||
      (directedGraph[table1] &&
        directedGraph[table1][table2] &&
        directedGraph[table1][table2][joinOn])
    ) {
      throw new Error('An invalid path was detected.');
    }
    if (!directedGraph[table1]) directedGraph[table1] = {};
    if (!directedGraph[table1][table2]) directedGraph[table1][table2] = {};
    directedGraph[table1][table2][joinOn] = joinCondition;
  }
  /**
   * Iterate through the table schema and add the edges to the directed graph.
   * The edges are added based on the join conditions provided in the table schema.
   * The SQL is split by the '=' sign and the tables columns involved in the joins are extracted.
   * The tables are then added as edges to the directed graph.
   */
  tableSchema.forEach((schema) => {
    schema?.joins?.forEach((join) => {
      const tables = join.sql.split('=').map((str) => str.split('.')[0].trim());
      const conditions = join.sql
        .split('=')
        .map((str) => str.split('.')[1].trim());

      /**
       * If the join SQL does not contain exactly 2 tables, then the join is invalid.
       */
      if (tables.length !== 2) {
        throw new Error(`Invalid join SQL: ${join.sql}`);
      }

      /**
       * If the tables are the same, then the join is invalid.
       */
      if (tables[0] === tables[1]) {
        throw new Error(`Invalid join SQL: ${join.sql}`);
      }

      /**
       * If the tables are not found in the table schema, then the join is invalid.
       */

      if (tables[0] !== schema.name && tables[1] !== schema.name) {
        throw new Error(
          `Table "${schema.name}" not found in provided join SQL: ${join.sql}`
        );
      }

      /**
       * Check if the tables are found in the table schema SQL map.
       */
      if (!tableSchemaSqlMap[tables[0]] || !tableSchemaSqlMap[tables[1]]) {
        return;
      }
      /**
       * If the table is the source table, then add the edge from the source to the target.
       * Thus find which table is the source and which is the target and add the edge accordingly.
       */
      if (tables[0] === schema.name) {
        addEdge(tables[0], tables[1], conditions[0], join.sql);
      } else {
        addEdge(tables[1], tables[0], conditions[1], join.sql);
      }
    });
  });

  return directedGraph;
};

function DFS(
  graph: any,
  node: string,
  visited: Set<string>,
  recStack: Set<string>
): boolean {
  visited.add(node);
  recStack.add(node);

  for (const neighbor in graph[node]) {
    if (!visited.has(neighbor) && DFS(graph, neighbor, visited, recStack)) {
      return true;
    } else if (recStack.has(neighbor)) {
      return true;
    }
  }

  recStack.delete(node);
  return false;
}

export function checkLoopInGraph(graph: any): boolean {
  const visited = new Set<string>();
  const recStack = new Set<string>();

  for (const node in graph) {
    if (DFS(graph, node, visited, recStack)) {
      return true;
    }
  }

  return false;
}

export const getCombinedTableSchema = async (
  tableSchema: TableSchema[],
  cubeQuery: Query
) => {
  if (tableSchema.length === 1) {
    return tableSchema[0];
  }

  const tableSchemaSqlMap = tableSchema.reduce(
    (acc: { [key: string]: string }, schema: TableSchema) => {
      return { ...acc, [schema.name]: schema.sql };
    },
    {}
  );

  const directedGraph = createDirectedGraph(tableSchema, tableSchemaSqlMap);
  const hasLoop = checkLoopInGraph(directedGraph);
  if (hasLoop) {
    throw new Error('A loop was detected in the joins.');
  }

  console.log('directedGraph', directedGraph);

  const baseSql = generateSqlQuery(
    cubeQuery.joinPath || [],
    tableSchemaSqlMap,
    directedGraph
  );

  const combinedTableSchema = tableSchema.reduce(
    (acc: TableSchema, schema: TableSchema) => {
      return {
        name: 'MEERKAT_GENERATED_TABLE',
        sql: baseSql,
        measures: [...acc.measures, ...schema.measures],
        dimensions: [...acc.dimensions, ...schema.dimensions],
        joins: [],
      };
    },
    {
      name: '',
      sql: '',
      measures: [],
      dimensions: [],
      joins: [],
    }
  );
  return combinedTableSchema;
};