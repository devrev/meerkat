import {
  astDeserializerQuery,
  deserializeQuery,
} from '../ast-deserializer/ast-deserializer';
import {
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  SelectNode,
} from '../types/duckdb-serialization-types';
import { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';
import { stripQueryLocationInPlace } from './helpers';

export async function buildBaseSQL(
  originalSql: string,
  selectNode: SelectNode,
  residualWhere: ParsedExpression | undefined,
  getQueryOutput: GetQueryOutput
): Promise<string> {
  const clonedNode = JSON.parse(JSON.stringify(selectNode));
  const fromOnlyNode = {
    ...clonedNode,
    select_list: [
      {
        class: ExpressionClass.STAR,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        relation_name: '',
        exclude_list: [],
        replace_list: [],
        columns: false,
      },
    ],
    where_clause: residualWhere
      ? JSON.parse(JSON.stringify(residualWhere))
      : undefined,
    group_expressions: [],
    group_sets: [],
    having: null,
    qualify: null,
    modifiers: [],
  };

  stripQueryLocationInPlace(fromOnlyNode);

  try {
    const query = astDeserializerQuery({ node: fromOnlyNode } as never);
    const rows = await getQueryOutput(query);
    const baseSql = deserializeQuery(rows).replace(/;\s*$/, '');
    return baseSql;
  } catch {
    const cleaned = originalSql.replace(/;\s*$/, '');
    return `SELECT * FROM (${cleaned}) AS _base`;
  }
}
