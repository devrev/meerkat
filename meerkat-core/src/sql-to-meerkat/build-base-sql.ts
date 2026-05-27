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
  selectNode: SelectNode,
  residualWhere: ParsedExpression | undefined,
  residualHaving: ParsedExpression | undefined,
  getQueryOutput: GetQueryOutput
): Promise<string | null> {
  const clonedNode = JSON.parse(JSON.stringify(selectNode));
  const hasResidualHaving = residualHaving !== undefined;

  const starSelectList = [
    {
      class: ExpressionClass.STAR,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      relation_name: '',
      exclude_list: [],
      replace_list: [],
      columns: false,
    },
  ];

  const fromOnlyNode = {
    ...clonedNode,
    select_list: hasResidualHaving ? clonedNode.select_list : starSelectList,
    where_clause: residualWhere
      ? JSON.parse(JSON.stringify(residualWhere))
      : undefined,
    group_expressions: hasResidualHaving ? clonedNode.group_expressions : [],
    group_sets: hasResidualHaving ? clonedNode.group_sets : [],
    having: hasResidualHaving
      ? JSON.parse(JSON.stringify(residualHaving))
      : null,
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
    return null;
  }
}
