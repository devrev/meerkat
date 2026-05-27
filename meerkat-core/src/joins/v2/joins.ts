import { getUsedTableSchema } from '../../get-used-table-schema/get-used-table-schema';
import { memberKeyToSafeKey } from '../../member-formatters/member-key-to-safe-key';
import { Graph, quoteIdentifierIfNeeded } from '../v1/joins';
import { Query, StructuredJoin, TableSchema } from '../../types/cube-types';

const UNNEST_ALIAS_PREFIX = '__mk_u_';
const ARRAY_DIMENSION_TYPES = new Set(['string_array', 'number_array']);

const findArrayMember = (
  tableSchemas: TableSchema[],
  table: string,
  column: string
) => {
  const schema = tableSchemas.find((s) => s.name === table);
  if (!schema) return undefined;
  const member =
    schema.dimensions.find((d) => d.name === column) ??
    schema.measures.find((m) => m.name === column);
  return member && ARRAY_DIMENSION_TYPES.has(member.type) ? member : undefined;
};

const isArrayColumn = (
  tableSchemas: TableSchema[],
  table: string,
  column: string
): boolean => findArrayMember(tableSchemas, table, column) !== undefined;

/**
 * Returns the SQL expression to feed `UNNEST(...)` for an array column
 * inside a `(SELECT *, UNNEST(<expr>) FROM (<baseSql>))` wrap. The
 * subquery exposes the table's columns directly (via `SELECT *`) but
 * the table alias is not yet in scope — so we strip all `${tableName}.`
 * qualifiers from `dim.sql`. After `ensureTableSchemasAlias`, the SQL
 * may be wrapped in expressions like `CAST(issue.col AS VARCHAR[])`,
 * so a simple `startsWith` prefix check is not sufficient.
 */
const getArrayUnnestExpression = (
  tableSchemas: TableSchema[],
  table: string,
  column: string
): string => {
  const member = findArrayMember(tableSchemas, table, column);
  const sql = member?.sql ?? column;
  const tablePrefix = `${table}.`;
  if (!sql.includes(tablePrefix)) return sql;
  // Replace all occurrences of `table.` that appear as word-boundary
  // qualified refs (not inside a string literal or as part of a longer name).
  const escaped = table.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return sql.replace(new RegExp(`\\b${escaped}\\.`, 'g'), '');
};

/**
 * For each path edge whose `from.column` is array-typed, record the
 * column under its source table. Multi-hop joins where the array column
 * is on an intermediate table (e.g. `ticket -> part -> tag` with
 * `part.tag_ids` as the array) require entries beyond the starting
 * table so each hop's right side gets its own UNNEST wrap.
 */
const collectArrayJoinSources = (
  paths: StructuredJoin[][],
  tableSchemas: TableSchema[]
): Map<string, Set<string>> => {
  const result = new Map<string, Set<string>>();
  for (const path of paths) {
    for (const { from } of path) {
      if (!isArrayColumn(tableSchemas, from.table, from.column)) continue;
      const cols = result.get(from.table) ?? new Set<string>();
      cols.add(from.column);
      result.set(from.table, cols);
    }
  }
  return result;
};

/**
 * Wraps a table's subquery with UNNEST projections for each array column
 * referenced as a join `from`. The unnested column is exposed under
 * `__mk_u_<col>` for use in the ON clause and re-projected with its
 * safe-key alias (`tableName____mk_u_<col>`) so the outer query can also
 * reference it.
 *
 * DuckDB cannot hash-join on `CONTAINS(...)`/`list_contains`, so without
 * the wrap, array joins fall back to O(n × m) nested-loop scans.
 */
/**
 * The unnested column name used inside the wrapped subquery. Single source of
 * truth for both the UNNEST projection and the predicate's left-hand side, so
 * the names emitted in the SELECT list stay in lock-step with the names
 * referenced in the ON clause.
 */
const getUnnestAlias = (column: string): string => `${UNNEST_ALIAS_PREFIX}${column}`;

/**
 * `<unnestAlias> AS <table.unnestAlias safe-key>` — re-aliases the unnested
 * column under the canonical safe-key form so outer queries can reference it
 * via the same `tableName____mk_u_<col>` identifier they use for any other
 * member.
 */
const buildSafeKeyAliasProjection = (tableName: string, column: string): string => {
  const unnestAlias = getUnnestAlias(column);
  return `${unnestAlias} AS ${memberKeyToSafeKey(`${tableName}.${unnestAlias}`)}`;
};

const wrapTableSqlForArrayFrom = (
  baseSql: string,
  tableName: string,
  arrayCols: Set<string> | undefined,
  tableSchemas: TableSchema[]
): string => {
  if (!arrayCols || arrayCols.size === 0) return baseSql;
  const cols = [...arrayCols];
  const unnestProjections = cols
    .map((c) => {
      const expr = getArrayUnnestExpression(tableSchemas, tableName, c);
      return `UNNEST(${expr}) AS ${getUnnestAlias(c)}`;
    })
    .join(', ');
  const aliasProjections = cols
    .map((c) => buildSafeKeyAliasProjection(tableName, c))
    .join(', ');
  const innerSql = `(SELECT *, ${unnestProjections} FROM (${baseSql}))`;
  return `(SELECT *, ${aliasProjections} FROM ${innerSql}) AS ${quoteIdentifierIfNeeded(
    tableName
  )}`;
};

const buildPredicate = (edge: StructuredJoin, fromIsArray: boolean): string => {
  const leftColumn = fromIsArray ? getUnnestAlias(edge.from.column) : edge.from.column;
  return `${edge.from.table}.${leftColumn} = ${edge.to.table}.${edge.to.column}`;
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
      graph[from.table] ??= {};
      graph[from.table][to.table] ??= {};
      graph[from.table][to.table][from.column] = buildPredicate(edge, fromIsArray);
    }
  }
  return graph;
};

/**
 * Builds the FROM clause for the combined table: the starting subquery
 * left-joined against each downstream table along every path. v2 differs
 * from v1 only in that any subquery whose source array column is joined
 * gets UNNEST projections so the join becomes a hash-joinable equi-join
 * (`base.__mk_u_col = right.col`) instead of a `CONTAINS(...)` scan.
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
  if (paths.some((p) => p[0]?.from.table !== startingTable)) {
    throw new Error(
      'Invalid path, starting node is not the same for all paths.'
    );
  }

  const arraySourcesByTable = collectArrayJoinSources(paths, tableSchemas);

  let query = wrapTableSqlForArrayFrom(
    tableSchemaSqlMap[startingTable],
    startingTable,
    arraySourcesByTable.get(startingTable),
    tableSchemas
  );

  const visited = new Map<string, StructuredJoin>();

  for (const path of paths) {
    for (const edge of path) {
      const prev = visited.get(edge.to.table);
      if (prev?.from.table === edge.from.table) continue;
      if (prev) {
        throw new Error(
          `Path ambiguity, node ${edge.to.table} visited from different sources`
        );
      }
      visited.set(edge.to.table, edge);

      const onClause =
        directedGraph[edge.from.table][edge.to.table][edge.from.column];
      const rightArrayCols = arraySourcesByTable.get(edge.to.table);
      const rightSubquery = rightArrayCols?.size
        ? wrapTableSqlForArrayFrom(
            tableSchemaSqlMap[edge.to.table],
            edge.to.table,
            rightArrayCols,
            tableSchemas
          )
        : `(${tableSchemaSqlMap[edge.to.table]}) AS ${quoteIdentifierIfNeeded(edge.to.table)}`;
      query += ` LEFT JOIN ${rightSubquery}  ON ${onClause}`;
    }
  }

  return query;
};

const hasLoop = (paths: StructuredJoin[][]): boolean => {
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

  // joinPathsV2 callers control which tables participate; everyone else
  // gets the legacy auto-prune via getUsedTableSchema.
  const activeTables = cubeQuery.joinPathsV2?.length
    ? tableSchema
    : getUsedTableSchema(tableSchema, cubeQuery);
  if (activeTables.length === 1) return activeTables[0];

  const paths = cubeQuery.joinPathsV2 ?? [];
  if (hasLoop(paths)) {
    throw new Error(
      `A loop was detected in the joins. ${JSON.stringify(paths)}`
    );
  }

  const tableSchemaSqlMap = Object.fromEntries(
    activeTables.map((s) => [s.name, s.sql])
  );
  const graph = createDirectedGraphV2(activeTables, tableSchemaSqlMap, paths);
  const sql = generateSqlQueryV2(paths, tableSchemaSqlMap, graph, activeTables);

  return {
    name: 'MEERKAT_GENERATED_TABLE',
    sql,
    measures: activeTables.flatMap((s) => s.measures),
    dimensions: activeTables.flatMap((s) => s.dimensions),
    joins: [],
  };
};
