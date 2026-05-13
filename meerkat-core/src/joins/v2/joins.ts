import { getUsedTableSchema } from '../../get-used-table-schema/get-used-table-schema';
import { Graph, quoteIdentifierIfNeeded } from '../v1/joins';
import { Query, StructuredJoin, TableSchema } from '../../types/cube-types';

const UNNEST_ALIAS_PREFIX = '__mk_u_';
const ARRAY_COLUMN_TYPES = new Set(['string_array', 'number_array']);

const isArrayColumn = (
  tableSchemas: TableSchema[],
  table: string,
  column: string
): boolean => {
  const t = tableSchemas.find((s) => s.name === table);
  if (!t) return false;
  const d = t.dimensions.find((x) => x.name === column);
  if (d) return ARRAY_COLUMN_TYPES.has(d.type);
  const m = t.measures.find((x) => x.name === column);
  return m ? ARRAY_COLUMN_TYPES.has(m.type) : false;
};

const buildPredicate = (edge: StructuredJoin, fromIsArray: boolean): string => {
  const left = fromIsArray
    ? `${edge.from.table}.${UNNEST_ALIAS_PREFIX}${edge.from.column}`
    : `${edge.from.table}.${edge.from.column}`;
  return `${left} = ${edge.to.table}.${edge.to.column}`;
};

/**
 * Wraps the base subquery with UNNEST projections for every array-typed
 * from-column referenced across the paths. DuckDB can't hash-join on
 * CONTAINS/list_contains, so without this wrap array joins fall back to
 * O(n × m).
 */
const wrapBaseSqlForArrayFrom = (
  baseSql: string,
  baseTable: string,
  paths: StructuredJoin[][],
  tableSchemas: TableSchema[]
): string => {
  const cols = new Set<string>();
  for (const path of paths) {
    for (const edge of path) {
      if (
        edge.from.table === baseTable &&
        isArrayColumn(tableSchemas, edge.from.table, edge.from.column)
      ) {
        cols.add(edge.from.column);
      }
    }
  }
  if (cols.size === 0) return baseSql;
  const projections = Array.from(cols)
    .map((c) => `UNNEST(${c}) AS ${UNNEST_ALIAS_PREFIX}${c}`)
    .join(', ');
  return `(SELECT *, ${projections} FROM (${baseSql})) AS ${quoteIdentifierIfNeeded(
    baseTable
  )}`;
};

export const createDirectedGraphV2 = (
  tableSchemas: TableSchema[],
  tableSchemaSqlMap: { [key: string]: string },
  joinPathsV2: StructuredJoin[][] | undefined
): Graph => {
  const graph: Graph = {};
  if (!joinPathsV2 || joinPathsV2.length === 0) return graph;

  for (const path of joinPathsV2) {
    for (const edge of path) {
      const { from, to } = edge;
      if (from.table === to.table) {
        throw new Error(`Invalid structured join: self-join on "${from.table}"`);
      }
      if (!tableSchemaSqlMap[from.table] || !tableSchemaSqlMap[to.table]) {
        continue;
      }
      const fromIsArray = isArrayColumn(tableSchemas, from.table, from.column);
      const toIsArray = isArrayColumn(tableSchemas, to.table, to.column);
      if (fromIsArray && toIsArray) {
        throw new Error(
          `array-array joins are not supported: ${from.table}.${from.column} -> ${to.table}.${to.column}`
        );
      }
      if (graph[from.table]?.[to.table]?.[from.column]) {
        throw new Error('An invalid path was detected.');
      }
      if (!graph[from.table]) graph[from.table] = {};
      if (!graph[from.table][to.table]) graph[from.table][to.table] = {};
      graph[from.table][to.table][from.column] = buildPredicate(edge, fromIsArray);
    }
  }
  return graph;
};

/**
 * Structurally identical to v1 `generateSqlQuery`. Only delta: when a
 * path's `from.column` is array-typed, the base subquery is wrapped with
 * UNNEST projections so the ON clause becomes a hash-joinable equi-join
 * (`base.__mk_u_col = right.col`) instead of a CONTAINS nested-loop.
 */
export const generateSqlQueryV2 = (
  paths: StructuredJoin[][],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph,
  tableSchemas: TableSchema[]
): string => {
  if (paths.length === 0) {
    throw new Error(
      'Invalid path, multiple data sources are present without a join path.'
    );
  }

  const startingTable = paths[0][0]?.from.table;
  if (!startingTable) return '';

  let query = wrapBaseSqlForArrayFrom(
    tableSchemaSqlMap[startingTable],
    startingTable,
    paths,
    tableSchemas
  );

  const visited = new Map<string, StructuredJoin>();

  for (let i = 0; i < paths.length; i++) {
    if (paths[i][0]?.from.table !== startingTable) {
      throw new Error(
        'Invalid path, starting node is not the same for all paths.'
      );
    }
    for (let j = 0; j < paths[i].length; j++) {
      const edge = paths[i][j];
      const prev = visited.get(edge.to.table);
      if (prev && prev.from.table === edge.from.table) continue;
      if (prev) {
        throw new Error(
          `Path ambiguity, node ${edge.to.table} visited from different sources`
        );
      }
      visited.set(edge.to.table, edge);

      const quotedAlias = quoteIdentifierIfNeeded(edge.to.table);
      query += ` LEFT JOIN (${
        tableSchemaSqlMap[edge.to.table]
      }) AS ${quotedAlias}  ON ${
        directedGraph[edge.from.table][edge.to.table][edge.from.column]
      }`;
    }
  }

  return query;
};

/** Mirrors v1's `checkLoopInJoinPath` — detects cycles within a path. */
const checkLoopInJoinPathV2 = (paths: StructuredJoin[][]): boolean => {
  for (const path of paths) {
    const visited = new Set<string>();
    if (path[0]) visited.add(path[0].from.table);
    for (const edge of path) {
      if (visited.has(edge.to.table)) return true;
      visited.add(edge.to.table);
    }
  }
  return false;
};

export const getCombinedTableSchemaV2 = (
  tableSchema: TableSchema[],
  cubeQuery: Query
): TableSchema => {
  if (tableSchema.length === 1) return tableSchema[0];

  const newTableSchema = cubeQuery.joinPathsV2?.length
    ? tableSchema
    : getUsedTableSchema(tableSchema, cubeQuery);

  if (newTableSchema.length === 1) return newTableSchema[0];

  const tableSchemaSqlMap = newTableSchema.reduce(
    (acc: { [key: string]: string }, s: TableSchema) => ({
      ...acc,
      [s.name]: s.sql,
    }),
    {}
  );

  const paths = cubeQuery.joinPathsV2 ?? [];
  if (checkLoopInJoinPathV2(paths)) {
    throw new Error(
      `A loop was detected in the joins. ${JSON.stringify(paths)}`
    );
  }
  const graph = createDirectedGraphV2(newTableSchema, tableSchemaSqlMap, paths);
  const baseSql = generateSqlQueryV2(paths, tableSchemaSqlMap, graph, newTableSchema);

  return newTableSchema.reduce(
    (acc: TableSchema, s: TableSchema) => ({
      name: 'MEERKAT_GENERATED_TABLE',
      sql: baseSql,
      measures: [...acc.measures, ...s.measures],
      dimensions: [...acc.dimensions, ...s.dimensions],
      joins: [],
    }),
    { name: '', sql: '', measures: [], dimensions: [], joins: [] }
  );
};

