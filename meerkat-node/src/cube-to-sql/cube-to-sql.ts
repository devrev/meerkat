import {
  BASE_TABLE_NAME,
  ContextParams,
  FilterType,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  detectApplyContextParamsToBaseSQL,
  getFilterParamsAST,
  getWrappedBaseQueryWithProjections,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const getFilterParamsSQL = async ({
  cubeQuery,
  tableSchema,
  filterType,
}: {
  cubeQuery: Query;
  tableSchema: TableSchema;
  filterType?: FilterType;
}) => {
  const filterParamsAST = getFilterParamsAST(
    cubeQuery,
    tableSchema,
    filterType
  );
  const filterParamsSQL = [];
  for (const filterParamAST of filterParamsAST) {
    if (!filterParamAST.ast) {
      continue;
    }

    const queryOutput = await duckdbExec<
      {
        [key: string]: string;
      }[]
    >(astDeserializerQuery(filterParamAST.ast));

    const sql = deserializeQuery(queryOutput);

    filterParamsSQL.push({
      memberKey: filterParamAST.memberKey,
      sql: sql,
      matchKey: filterParamAST.matchKey,
    });
  }
  return filterParamsSQL;
};

const getFinalBaseSQL = async (cubeQuery: Query, tableSchema: TableSchema) => {
  /**
   * Apply transformation to the supplied base query.
   * This involves updating the filter placeholder with the actual filter values.
   */
  const baseFilterParamsSQL = await getFilterParamsSQL({
    cubeQuery: cubeQuery,
    tableSchema,
    filterType: 'BASE_FILTER',
  });
  const baseSQL = applyFilterParamsToBaseSQL(
    tableSchema.sql,
    baseFilterParamsSQL
  );
  const baseSQLWithFilterProjection = getWrappedBaseQueryWithProjections({
    baseQuery: baseSQL,
    tableSchema,
    query: cubeQuery,
  });
  return baseSQLWithFilterProjection;
};

function generateSqlQuery(
  path: string[],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph
): string {
  let query: string = `${tableSchemaSqlMap[path[0]]}`;

  console.info('tableSchemaSqlMap', tableSchemaSqlMap);
  console.info('directedGraph', directedGraph);

  for (let i = 1; i < path.length; i++) {
    console.info('path[i]', path[i]);
    console.info('path[i - 1]', path[i - 1]);
    query += ` LEFT JOIN (${tableSchemaSqlMap[path[i]]}) ON ${
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
  let directedGraph: { [key: string]: { [key: string]: string } } = {};

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
    schema.joins.forEach((join) => {
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

type Graph = { [key: string]: { [key: string]: string } };

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

const getCombinedTableSchema = async (tableSchema: TableSchema[]) => {
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

export const cubeQueryToSQL = async (
  cubeQuery: Query,
  tableSchemas: TableSchema[],
  contextParams?: ContextParams
) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL(cubeQuery, schema);
      console.info(
        `baseFilterParamsSQL for schema ${schema.name}: `,
        baseFilterParamsSQL
      );
      return {
        ...schema,
        sql: baseFilterParamsSQL,
      };
    })
  );

  const updatedTableSchema = await getCombinedTableSchema(updatedTableSchemas);

  const ast = cubeToDuckdbAST(cubeQuery, updatedTableSchema);
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(queryTemp);
  const preBaseQuery = deserializeQuery(queryOutput);

  const filterParamsSQL = await getFilterParamsSQL({
    cubeQuery,
    tableSchema: updatedTableSchema,
  });

  const filterParamQuery = applyFilterParamsToBaseSQL(
    updatedTableSchema.sql,
    filterParamsSQL
  );

  /**
   * Replace CONTEXT_PARAMS with context params
   */
  const baseQuery = detectApplyContextParamsToBaseSQL(
    filterParamQuery,
    contextParams || {}
  );

  /**
   * Replace BASE_TABLE_NAME with cube query
   */
  const replaceBaseTableName = preBaseQuery.replace(
    BASE_TABLE_NAME,
    `(${baseQuery}) AS ${updatedTableSchema.name}`
  );

  /**
   * Add measures to the query
   */
  const measures = cubeQuery.measures;
  const dimensions = cubeQuery.dimensions || [];
  const finalQuery = applyProjectionToSQLQuery(
    dimensions,
    measures,
    updatedTableSchema,
    replaceBaseTableName
  );

  return finalQuery;
};
