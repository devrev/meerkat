import { astSerializerQuery } from '../../ast-serializer/ast-serializer';
import { deserializeQuery } from '../../ast-deserializer/ast-deserializer';
import { QueryNodeType, SelectNode } from '../../types/duckdb-serialization-types';
import { GetQueryOutput } from '../../utils/duckdb-ast-parse-serialize';
import { sanitizeStringValue } from '../../member-formatters/sanitize-value';
import { DuckDBSerializedAST } from '../types';
import { extractTableName, hasRecursiveCteInMap } from '../helpers';

export interface ParseResult {
  selectNode: SelectNode;
  tableName: string;
}

/**
 * Parses raw SQL into a validated DuckDB AST SelectNode.
 * Returns failure reason string on error, or ParseResult on success.
 */
export async function parseAndValidateAst(
  sql: string,
  getQueryOutput: GetQueryOutput
): Promise<ParseResult | string> {
  // Parse SQL via DuckDB's json_serialize_sql (syntactic only, no binding)
  let parsedAst: DuckDBSerializedAST;
  try {
    const serializeQuery = astSerializerQuery(sanitizeStringValue(sql));
    const rows = await getQueryOutput(serializeQuery);
    const jsonStr = deserializeQuery(rows);
    parsedAst = JSON.parse(jsonStr) as DuckDBSerializedAST;
  } catch (e) {
    return `DuckDB parse failed: ${(e as Error).message}`;
  }

  // json_serialize_sql sets error=true for syntax errors
  if (parsedAst.error) {
    return `DuckDB parse failed: ${parsedAst.error_message || 'unknown error'}`;
  }

  // Extract first statement
  const statement = parsedAst.statements?.[0];
  if (!statement?.node) {
    return 'No statement found in parsed AST';
  }

  const node = statement.node;

  // Reject unsupported node types
  if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    return 'UNION/INTERSECT/EXCEPT not supported';
  }
  if (node.type !== QueryNodeType.SELECT_NODE) {
    return `Unsupported query type: ${node.type}`;
  }

  const selectNode = node as SelectNode;

  // Reject WITH RECURSIVE
  if (hasRecursiveCteInMap(selectNode)) {
    return 'WITH RECURSIVE not supported';
  }

  const tableName = extractTableName(selectNode);
  return { selectNode, tableName };
}
