import { TableSchema } from '../types/cube-types';

type Graph = { [key: string]: { [key: string]: string } };

export function generateSqlQuery(
  path: string[],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph
): string {
  let query = `${tableSchemaSqlMap[path[0]]}`;

  /**
   * The path array contains the nodes in the order they are connected.
   * As the paths are in pairs of edges, the edge is only between the nodes at even index and next odd index.
   * So, we do i+=2 to get the edge between the nodes.
   * Example path: ['table1', 'table2', 'table2', 'table3', 'table3', 'table4']
   * The edges are: table1->table2, table2->table3, table3->table4
   */

  for (let i = 1; i < path.length; i += 2) {
    query += ` LEFT JOIN (${tableSchemaSqlMap[path[i]]}) AS ${path[i]}  ON ${
      directedGraph[path[i - 1]][path[i]]
    }`;
  }

  return query;
}

export const getJoinPathAsArray = (
  startingNode: string,
  graph: Graph
): string[] => {
  const queue = [startingNode];
  const visitedNodes = new Set<string>();
  const path: string[] = [];

  while (queue.length) {
    const currentNode = queue.shift() as string;
    visitedNodes.add(currentNode);

    for (const connectedNode in graph[currentNode]) {
      if (!visitedNodes.has(connectedNode)) {
        queue.push(connectedNode);

        path.push(currentNode); // This node is the source node for direct edge
        path.push(connectedNode); // This node is the target node for direct edge
      }
    }
  }

  return path;
};

export const getStartingNodes = (graph: Graph): string[] => {
  const incomingEdgesCount: { [key: string]: number } = {};

  for (const sourceNode in graph) {
    if (!(sourceNode in incomingEdgesCount)) {
      incomingEdgesCount[sourceNode] = 0;
    }

    for (const targetNode in graph[sourceNode]) {
      if (!(targetNode in incomingEdgesCount)) {
        incomingEdgesCount[targetNode] = 0;
      }

      incomingEdgesCount[targetNode]++;
    }
  }

  return Object.keys(incomingEdgesCount).filter(
    (node) => incomingEdgesCount[node] === 0
  );
};

export const createDirectedGraph = (
  tableSchema: TableSchema[],
  tableSchemaSqlMap: { [key: string]: string }
) => {
  const directedGraph: { [key: string]: { [key: string]: string } } = {};

  function addEdge(table1: string, table2: string, joinCondition: string) {
    if (
      table1 === table2 ||
      (directedGraph[table1] && directedGraph[table1][table2])
    ) {
      throw new Error('An invalid path was detected.');
    }
    if (!directedGraph[table1]) directedGraph[table1] = {};
    directedGraph[table1][table2] = joinCondition;
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
        addEdge(tables[0], tables[1], join.sql);
      } else {
        addEdge(tables[1], tables[0], join.sql);
      }
    });
  });

  return directedGraph;
};

export const checkLoopInGraph = (graph: Graph): boolean | Error => {
  const visited = new Set<string>();
  const recursionStack = new Set<string>();

  // depth-first search
  function DFS(node: string): boolean {
    visited.add(node);
    recursionStack.add(node);

    for (const connectedNode in graph[node]) {
      if (!visited.has(connectedNode)) {
        const hasCycle = DFS(connectedNode);
        if (hasCycle) return true;
      } else if (recursionStack.has(connectedNode)) {
        return true;
      }
    }

    // remove the node from the recursion stack after all descendants have been visited
    recursionStack.delete(node);
    return false;
  }

  for (const node in graph) {
    if (!visited.has(node)) {
      const hasCycle = DFS(node);
      if (hasCycle) return true;
    }
  }

  return false;
};

export const getCombinedTableSchema = async (tableSchema: TableSchema[]) => {
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

  const startingNodes = getStartingNodes(directedGraph);

  if (startingNodes.length === 0) {
    throw new Error('No starting nodes found in the graph.');
  }

  if (startingNodes.length > 1) {
    throw new Error('Multiple starting nodes found in the graph.');
  }

  const joinPath = getJoinPathAsArray(startingNodes[0], directedGraph);

  const baseSql = generateSqlQuery(joinPath, tableSchemaSqlMap, directedGraph);

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
