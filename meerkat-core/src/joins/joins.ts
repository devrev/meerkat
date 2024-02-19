import { TableSchema } from '../types/cube-types';

type Graph = { [key: string]: { [key: string]: string } };

function generateSqlQuery(
  path: string[],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph
): string {
  let query = `${tableSchemaSqlMap[path[0]]}`;

  console.info('tableSchemaSqlMap', tableSchemaSqlMap);
  console.info('directedGraph', directedGraph);

  for (let i = 1; i < path.length; i++) {
    console.info('path[i]', path[i]);
    console.info('path[i - 1]', path[i - 1]);
    query += ` LEFT JOIN (${tableSchemaSqlMap[path[i]]}) AS ${path[i]}  ON ${
      directedGraph[path[i - 1]][path[i]]
    }`;
  }

  return query;
}

const getJoinPathAsArray = (startingNode: string, graph: Graph): string[] => {
  function DFS(node: string, visited: Set<string>, path: string[]): string[] {
    visited.add(node);
    path.push(node);

    for (const connectedNode in graph[node]) {
      if (!visited.has(connectedNode)) {
        DFS(connectedNode, visited, path);
      }
    }
    return path;
  }

  const visitedNodes = new Set<string>();
  const path: string[] = [];

  return DFS(startingNode, visitedNodes, path);
};

const getStartingNodes = (graph: Graph): string[] => {
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

const createDirectedGraph = (tableSchema: TableSchema[]) => {
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

  tableSchema.forEach((schema) => {
    schema?.joins?.forEach((join) => {
      const tables = join.sql.split('=').map((str) => str.split('.')[0].trim());

      if (tables.length !== 2) {
        throw new Error(`Invalid join SQL: ${join.sql}`);
      }

      if (tables[0] === tables[1]) {
        throw new Error(`Invalid join SQL: ${join.sql}`);
      }

      if (tables[0] !== schema.name && tables[1] !== schema.name) {
        throw new Error(
          `Table "${schema.name}" not found in provided join SQL: ${join.sql}`
        );
      }

      if (tables[0] === schema.name) {
        addEdge(tables[0], tables[1], join.sql);
      } else {
        addEdge(tables[1], tables[0], join.sql);
      }
    });
  });

  return directedGraph;
};

const checkLoopInGraph = (graph: Graph): boolean | Error => {
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

  const directedGraph = createDirectedGraph(tableSchema);
  const hasLoop = checkLoopInGraph(directedGraph);
  if (hasLoop) {
    throw new Error('A loop was detected in the joins.');
  }

  const tableSchemaSqlMap = tableSchema.reduce(
    (acc: { [key: string]: string }, schema: TableSchema) => {
      return { ...acc, [schema.name]: schema.sql };
    },
    {}
  );

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
        name: 'joined_table',
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
