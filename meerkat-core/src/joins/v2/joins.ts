import { cubeFilterToDuckdbAST } from '../../cube-filter-transformer/factory';
import { traverseMeerkatQueryFilter } from '../../filter-params/filter-params-ast';
import { getUsedTableSchema } from '../../get-used-table-schema/get-used-table-schema';
import { memberKeyToSafeKey } from '../../member-formatters/member-key-to-safe-key';
import { splitIntoDataSourceAndFields } from '../../member-formatters/split-into-data-source-and-fields';
import {
  MeerkatQueryFilter,
  Query,
  StructuredJoin,
  TableSchema,
} from '../../types/cube-types';
import { ParsedExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { getBaseAST } from '../../utils/base-ast';
import { cubeFiltersEnrichment } from '../../utils/cube-filter-enrichment';
import {
  GetQueryOutput,
  serializeExpressions,
} from '../../utils/duckdb-ast-parse-serialize';
import { Graph, quoteIdentifierIfNeeded } from '../v1/joins';

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
const getUnnestAlias = (column: string): string =>
  `${UNNEST_ALIAS_PREFIX}${column}`;

/**
 * `<unnestAlias> AS <table.unnestAlias safe-key>` — re-aliases the unnested
 * column under the canonical safe-key form so outer queries can reference it
 * via the same `tableName____mk_u_<col>` identifier they use for any other
 * member.
 */
const buildSafeKeyAliasProjection = (
  tableName: string,
  column: string
): string => {
  const unnestAlias = getUnnestAlias(column);
  return `${unnestAlias} AS ${memberKeyToSafeKey(
    `${tableName}.${unnestAlias}`
  )}`;
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

const buildEquiJoinPredicate = (
  edge: StructuredJoin,
  fromIsArray: boolean
): string => {
  const leftColumn = fromIsArray
    ? getUnnestAlias(edge.from.column)
    : edge.from.column;
  return `${edge.from.table}.${leftColumn} = ${edge.to.table}.${edge.to.column}`;
};
const conditionFilterToAST = (
  condition: MeerkatQueryFilter,
  tableSchema: TableSchema | undefined
): ParsedExpression | null => {
  if (!tableSchema) return null;
  const filters = [JSON.parse(JSON.stringify(condition))];
  const enriched = cubeFiltersEnrichment(filters, tableSchema);
  if (!enriched) return null;
  return (
    cubeFilterToDuckdbAST(enriched, getBaseAST(), { isAlias: false }) ?? null
  );
};

const getReferencedTables = (cubeQuery: Query): Set<string> => {
  const tables = new Set<string>();
  const extractTable = (member: string) => {
    const dotIndex = member.indexOf('.');
    if (dotIndex > 0) tables.add(member.slice(0, dotIndex));
  };

  cubeQuery.measures.forEach(extractTable);
  cubeQuery.dimensions?.forEach(extractTable);
  if (cubeQuery.filters) {
    traverseMeerkatQueryFilter(cubeQuery.filters, (filter) => {
      extractTable(filter.member);
    });
  }
  return tables;
};

const inferBridgeTables = (
  paths: StructuredJoin[][],
  cubeQuery: Query
): Set<string> => {
  const referenced = getReferencedTables(cubeQuery);
  const bridges = new Set<string>();
  for (const path of paths) {
    for (const edge of path) {
      if (!referenced.has(edge.to.table)) {
        bridges.add(edge.to.table);
      }
    }
  }
  return bridges;
};

/**
 * When a bridge table is aliased (e.g. `link` → `link__1`), rewrite any
 * condition members that still qualify columns with the original table name
 * so ON-clause filters bind to the aliased instance, not the first join.
 *
 * Without this, equi-joins correctly use `link__1.target_id` while the
 * condition still references `link.link_type_id` — always FALSE against the
 * first link's type — so the second linked path never resolves.
 */
const rewriteConditionTableAlias = (
  condition: MeerkatQueryFilter,
  originalTable: string,
  aliasedTable: string
): MeerkatQueryFilter => {
  const rewritten: MeerkatQueryFilter = JSON.parse(JSON.stringify(condition));
  traverseMeerkatQueryFilter([rewritten], (filter) => {
    const [table, fields] = splitIntoDataSourceAndFields(filter.member);
    if (table === originalTable) {
      filter.member = `${aliasedTable}.${fields}`;
    }
  });
  return rewritten;
};

const aliasBridgeTables = (
  paths: StructuredJoin[][],
  bridgeTables: Set<string>
): StructuredJoin[][] => {
  const seen = new Map<string, number>();
  if (paths[0]?.[0]) seen.set(paths[0][0].from.table, 1);

  return paths.map((path) => {
    const result: StructuredJoin[] = [];
    let nextFromAlias: string | undefined;

    for (let i = 0; i < path.length; i++) {
      let edge = path[i];
      if (nextFromAlias) {
        edge = { ...edge, from: { ...edge.from, table: nextFromAlias } };
        nextFromAlias = undefined;
      }

      const count = seen.get(edge.to.table) ?? 0;
      seen.set(edge.to.table, count + 1);

      if (count === 0 || !bridgeTables.has(edge.to.table)) {
        result.push(edge);
        continue;
      }

      const originalTo = edge.to.table;
      const alias = `${originalTo}__${count}`;
      result.push({
        ...edge,
        to: { ...edge.to, table: alias },
        ...(edge.condition
          ? {
              condition: rewriteConditionTableAlias(
                edge.condition,
                originalTo,
                alias
              ),
            }
          : {}),
      });

      if (i + 1 < path.length && path[i + 1].from.table === originalTo) {
        nextFromAlias = alias;
      }
    }
    return result;
  });
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
        throw new Error(
          `Invalid structured join: self-join on "${from.table}"`
        );
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
        continue;
      }
      graph[from.table] ??= {};
      graph[from.table][to.table] ??= {};
      graph[from.table][to.table][from.column] = buildEquiJoinPredicate(
        edge,
        fromIsArray
      );
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
export const generateSqlQueryV2 = async (
  paths: StructuredJoin[][],
  tableSchemaSqlMap: { [key: string]: string },
  directedGraph: Graph,
  tableSchemas: TableSchema[],
  getQueryOutput?: GetQueryOutput,
  bridgeTables?: Set<string>
): Promise<string> => {
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

  const resolvedPaths = aliasBridgeTables(paths, bridgeTables ?? new Set());

  const visited = new Map<string, StructuredJoin>();
  const edgeOrder: { edge: StructuredJoin; equiJoin: string }[] = [];
  const conditionASTs: ParsedExpression[] = [];
  const conditionIndexByEdge: number[] = [];

  for (const path of resolvedPaths) {
    for (const edge of path) {
      const prev = visited.get(edge.to.table);
      if (prev) {
        const prevFrom = prev.from.table.replace(/__\d+$/, '');
        const currFrom = edge.from.table.replace(/__\d+$/, '');
        if (prevFrom === currFrom) continue;
        throw new Error(
          `Path ambiguity, node ${edge.to.table} visited from different sources`
        );
      }
      visited.set(edge.to.table, edge);

      const equiJoin =
        directedGraph[edge.from.table]?.[edge.to.table]?.[edge.from.column] ??
        buildEquiJoinPredicate(
          edge,
          isArrayColumn(tableSchemas, edge.from.table, edge.from.column)
        );

      edgeOrder.push({ edge, equiJoin });

      if (edge.condition) {
        const toTable = edge.to.table;
        const toTableOriginal = toTable.replace(/__\d+$/, '');
        const toTableSchema = tableSchemas.find(
          (s) => s.name === toTable || s.name === toTableOriginal
        );
        const ast = conditionFilterToAST(edge.condition, toTableSchema);
        if (ast) {
          conditionIndexByEdge.push(conditionASTs.length);
          conditionASTs.push(ast);
        } else {
          conditionIndexByEdge.push(-1);
        }
      } else {
        conditionIndexByEdge.push(-1);
      }
    }
  }

  let conditionSqls: string[] = [];
  if (conditionASTs.length > 0) {
    if (!getQueryOutput) {
      throw new Error(
        'getQueryOutput is required when join edges have filter conditions'
      );
    }
    conditionSqls = await serializeExpressions(conditionASTs, getQueryOutput);
  }

  for (let i = 0; i < edgeOrder.length; i++) {
    const { edge, equiJoin } = edgeOrder[i];
    const condIdx = conditionIndexByEdge[i];
    const onClause =
      condIdx >= 0 ? `${equiJoin} AND ${conditionSqls[condIdx]}` : equiJoin;

    const toTable = edge.to.table;
    const toTableOriginal = toTable.replace(/__\d+$/, '');
    const toTableSql =
      tableSchemaSqlMap[toTable] ?? tableSchemaSqlMap[toTableOriginal];

    const rightArrayCols = arraySourcesByTable.get(toTable);
    const rightSubquery = rightArrayCols?.size
      ? wrapTableSqlForArrayFrom(
          toTableSql,
          toTable,
          rightArrayCols,
          tableSchemas
        )
      : `(${toTableSql}) AS ${quoteIdentifierIfNeeded(toTable)}`;
    query += ` LEFT JOIN ${rightSubquery}  ON ${onClause}`;
  }

  return query;
};

const hasLoop = (
  paths: StructuredJoin[][],
  bridgeTables: Set<string>
): boolean => {
  for (const path of paths) {
    const visited = new Set<string>();
    if (path[0]) visited.add(path[0].from.table);
    for (const edge of path) {
      if (bridgeTables.has(edge.to.table)) continue;
      if (visited.has(edge.to.table)) return true;
      visited.add(edge.to.table);
    }
  }
  return false;
};

export const getCombinedTableSchemaV2 = async (
  tableSchema: TableSchema[],
  cubeQuery: Query,
  getQueryOutput?: GetQueryOutput
): Promise<TableSchema> => {
  if (tableSchema.length === 1) return tableSchema[0];

  // joinPathsV2 callers control which tables participate; everyone else
  // gets the legacy auto-prune via getUsedTableSchema.
  const activeTables = cubeQuery.joinPathsV2?.length
    ? tableSchema
    : getUsedTableSchema(tableSchema, cubeQuery);
  if (activeTables.length === 1) return activeTables[0];

  const paths = cubeQuery.joinPathsV2 ?? [];
  const bridgeTables = inferBridgeTables(paths, cubeQuery);
  if (hasLoop(paths, bridgeTables)) {
    throw new Error(
      `A loop was detected in the joins. ${JSON.stringify(paths)}`
    );
  }

  const tableSchemaSqlMap = Object.fromEntries(
    activeTables.map((s) => [s.name, s.sql])
  );
  const graph = createDirectedGraphV2(activeTables, tableSchemaSqlMap, paths);
  const sql = await generateSqlQueryV2(
    paths,
    tableSchemaSqlMap,
    graph,
    activeTables,
    getQueryOutput,
    bridgeTables
  );

  return {
    name: 'MEERKAT_GENERATED_TABLE',
    sql,
    measures: activeTables.flatMap((s) => s.measures),
    dimensions: activeTables.flatMap((s) => s.dimensions),
    joins: [],
  };
};
