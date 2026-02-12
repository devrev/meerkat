import { getUsedTableSchema } from '../get-used-table-schema/get-used-table-schema';
import { JoinPath, Query, TableSchema, isJoinNode } from '../types/cube-types';

/**
 * Regex pattern to match a CONTAINS function call in join SQL.
 *
 * Matches: CONTAINS(arg1, arg2)
 *
 * Pattern breakdown:
 * - ^\s*           : Start of string, optional leading whitespace
 * - CONTAINS       : Literal "CONTAINS" (case-insensitive due to 'i' flag)
 * - \s*\(\s*       : Opening parenthesis with optional whitespace
 * - (.+?)         : Capture group 1 - first argument (non-greedy)
 * - \s*,\s*        : Comma separator with optional whitespace
 * - (.+?)         : Capture group 2 - second argument (non-greedy)
 * - \s*\)\s*$      : Closing parenthesis with optional whitespace, end of string
 *
 * Examples that match:
 * - "CONTAINS(table1.col, table2.col)"
 * - "  contains( table1.items , table2.id )  "
 * - "CONTAINS(\"quoted.table\".col, other.col)"
 */
const CONTAINS_FUNCTION_REGEX = /^\s*CONTAINS\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)\s*$/i;

/**
 * Regex pattern to validate table.column reference format.
 *
 * Matches: tableName.columnName or "quotedTableName".columnName
 * Also supports composite fields with multiple dots (e.g., table.field.subfield)
 *
 * Pattern breakdown:
 * - ^               : Start of string
 * - (?:             : Non-capturing group for table name alternatives:
 *   - "[^"]+"       : Quoted identifier (e.g., "table.name")
 *   - |             : OR
 *   - [^.]+         : Unquoted identifier (any chars except dot)
 * - )               : End of non-capturing group
 * - \.              : Literal dot separator
 * - .+              : Column/field path (one or more chars, allows dots for composite fields)
 * - $               : End of string
 *
 * Examples that match:
 * - "table1.column"
 * - "myTable.myColumn"
 * - "\"quoted.table\".column"
 * - "table.field.subfield" (composite field)
 * - "table.nested.deep.field" (deeply nested composite field)
 *
 * Examples that do NOT match:
 * - "just_a_column" (no dot)
 * - ".column" (empty table name)
 */
const TABLE_COLUMN_REFERENCE_REGEX = /^(?:"[^"]+"|[^.]+)\..+$/;

export type Graph = {
  [key: string]: { [key: string]: { [key: string]: string } };
};

/**
 * Quotes an identifier if it contains dots or other special characters.
 * Used for table aliases in SQL that may contain dots when useDotNotation is true.
 *
 * @param identifier - The identifier to potentially quote
 * @returns The identifier, quoted if it contains dots
 */
export function quoteIdentifierIfNeeded(identifier: string): string {
  return identifier.includes('.') ? `"${identifier}"` : identifier;
}

/**
 * Parses a table.column reference that may include quoted identifiers.
 * Handles formats like:
 * - `tableName.columnName` (simple)
 * - `"tableName".columnName` (quoted table)
 * - `"table.name".columnName` (quoted table with dots)
 *
 * @param reference - The full table.column reference string
 * @returns An object with the table name and column name
 */
export function parseTableColumnReference(reference: string): {
  tableName: string;
  columnName: string;
} {
  const trimmed = reference.trim();

  // Check if the table name is quoted (starts with ")
  if (trimmed.startsWith('"')) {
    // Find the closing quote
    const closeQuoteIndex = trimmed.indexOf('"', 1);
    if (closeQuoteIndex === -1) {
      throw new Error(`Invalid quoted identifier: ${reference}`);
    }

    // Extract the table name (without quotes)
    const tableName = trimmed.substring(1, closeQuoteIndex);
    // The column name comes after the closing quote and a dot
    const columnName = trimmed.substring(closeQuoteIndex + 2);

    return { tableName, columnName };
  }

  // Simple case: no quotes, just split by first dot
  const dotIndex = trimmed.indexOf('.');
  if (dotIndex === -1) {
    throw new Error(`Invalid table.column reference: ${reference}`);
  }

  return {
    tableName: trimmed.substring(0, dotIndex),
    columnName: trimmed.substring(dotIndex + 1),
  };
}

export function generateSqlQuery(
  path: JoinPath[],
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

  /**
   * If the starting node is not a join node, then return the query as is.
   * It means that the query is a single node query.
   */
  if (!isJoinNode(path[0][0])) {
    return query;
  }

  const visitedNodes = new Map();

  for (let i = 0; i < path.length; i++) {
    if (path[i][0].left !== startingNode) {
      throw new Error(
        'Invalid path, starting node is not the same for all paths.'
      );
    }
    for (let j = 0; j < path[i].length; j++) {
      const currentEdge = path[i][j];

      if (!isJoinNode(currentEdge)) {
        continue;
      }

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

      // Quote the alias if it contains dots (useDotNotation mode)
      const quotedAlias = quoteIdentifierIfNeeded(currentEdge.right);
      query += ` LEFT JOIN (${
        tableSchemaSqlMap[currentEdge.right]
      }) AS ${quotedAlias}  ON ${
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

  /**
   * Parses a join SQL condition and extracts the table and column names
   * from each side of the binary operator.
   *
   * Supported forms (whitespace and case-insensitive for operators):
   * - tableA.column = tableB.column
   * - CONTAINS(tableA.column, tableB.column)
   *
   * The full, original join SQL string is still stored as the join condition
   * in the directed graph; this helper is only responsible for figuring out
   * which tables and columns participate in the join.
   */
  function parseJoinSqlColumns(joinSql: string): {
    tables: [string, string];
    conditions: [string, string];
  } {
    let leftPart: string;
    let rightPart: string;

    const containsFnMatch = joinSql.match(CONTAINS_FUNCTION_REGEX);

    if (containsFnMatch) {
      leftPart = containsFnMatch[1];
      rightPart = containsFnMatch[2];

      // Validate that both arguments follow the table.column structure
      if (!TABLE_COLUMN_REFERENCE_REGEX.test(leftPart.trim())) {
        throw new Error(
          `Invalid CONTAINS argument: "${leftPart}". Expected format: table.column`
        );
      }
      if (!TABLE_COLUMN_REFERENCE_REGEX.test(rightPart.trim())) {
        throw new Error(
          `Invalid CONTAINS argument: "${rightPart}". Expected format: table.column`
        );
      }
    } else {
      // Try equality join: tableA.col = tableB.col
      const equalsParts = joinSql.split('=');

      if (equalsParts.length === 2) {
        [leftPart, rightPart] = equalsParts;
      } else {
        throw new Error(`Invalid join SQL: ${joinSql}`);
      }
    }

    const leftRef = parseTableColumnReference(leftPart);
    const rightRef = parseTableColumnReference(rightPart);

    return {
      tables: [leftRef.tableName, rightRef.tableName],
      conditions: [leftRef.columnName, rightRef.columnName],
    };
  }

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
   * The SQL is parsed to extract the tables and columns involved in the joins,
   * supporting both '=' equality joins and CONTAINS(arg1, arg2) function syntax.
   * The tables are then added as edges to the directed graph.
   * Supports quoted identifiers for table names that contain dots (e.g., "table.name".column).
   */
  tableSchema.forEach((schema) => {
    schema?.joins?.forEach((join) => {
      // Parse the table and column from each side of the join condition.
      // Supports both equality joins (tableA.col = tableB.col) and
      // CONTAINS-style joins (tableA.col contains tableB.col).
      const { tables, conditions } = parseJoinSqlColumns(join.sql);

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

export const checkLoopInJoinPath = (joinPath: JoinPath[]) => {
  for (let i = 0; i < joinPath.length; i++) {
    const visitedNodes = new Set<string>();
    const currentJoinPath = joinPath[i];
    visitedNodes.add(currentJoinPath[0].left);
    for (let j = 0; j < currentJoinPath.length; j++) {
      const currentEdge = currentJoinPath[j];
      if (isJoinNode(currentEdge) && visitedNodes.has(currentEdge.right)) {
        if (visitedNodes.has(currentEdge.right)) {
          return true;
        }
        visitedNodes.add(currentEdge.right);
      }
    }
  }
  return false;
};

export const getCombinedTableSchema = (
  tableSchema: TableSchema[],
  cubeQuery: Query
) => {
  if (tableSchema.length === 1) {
    return tableSchema[0];
  }

  const newTableSchema: TableSchema[] = getUsedTableSchema(
    tableSchema,
    cubeQuery
  );

  if (newTableSchema.length === 1) {
    return newTableSchema[0];
  }

  const tableSchemaSqlMap = newTableSchema.reduce(
    (acc: { [key: string]: string }, schema: TableSchema) => {
      return { ...acc, [schema.name]: schema.sql };
    },
    {}
  );

  const directedGraph = createDirectedGraph(newTableSchema, tableSchemaSqlMap);
  const hasLoop = checkLoopInJoinPath(cubeQuery.joinPaths || []);

  if (hasLoop) {
    throw new Error(
      `A loop was detected in the joins. ${JSON.stringify(
        cubeQuery.joinPaths || []
      )}`
    );
  }

  const baseSql = generateSqlQuery(
    cubeQuery.joinPaths || [],
    tableSchemaSqlMap,
    directedGraph
  );

  const combinedTableSchema = newTableSchema.reduce(
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
