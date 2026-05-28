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

/**
 * Constructs the base SQL from the original AST, stripping extracted components.
 *
 * The base SQL is what cubeQueryToSQL wraps as a subquery. It contains:
 * - FROM / JOIN clauses (always preserved)
 * - Residual WHERE conditions that couldn't be extracted as filters
 * - Residual HAVING when measure filters can't be matched
 * - QUALIFY clause (preserved with original select list for window alias resolution)
 * - CTE definitions (preserved via cte_map)
 *
 * Stripped: ORDER BY, LIMIT, OFFSET, SAMPLE, DISTINCT (handled by Query layer).
 * When no residual HAVING/QUALIFY: select list becomes SELECT * (all columns available).
 */

export async function buildBaseSQL(
  selectNode: SelectNode,
  residualWhere: ParsedExpression | undefined,
  residualHaving: ParsedExpression | undefined,
  getQueryOutput: GetQueryOutput,
  selectListAliases?: readonly string[]
): Promise<string | null> {
  const clonedNode = JSON.parse(JSON.stringify(selectNode));
  const hasResidualHaving = residualHaving !== undefined;
  const hasQualify = !!clonedNode.qualify;
  const keepOriginalSelect = hasResidualHaving || hasQualify;

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

  let selectList = keepOriginalSelect ? clonedNode.select_list : starSelectList;
  if (keepOriginalSelect && selectListAliases) {
    selectList = selectList.map((expr: ParsedExpression, i: number) => {
      if (selectListAliases[i]) {
        return { ...expr, alias: selectListAliases[i] };
      }
      return expr;
    });
  }

  const fromOnlyNode = {
    ...clonedNode,
    select_list: selectList,
    where_clause: residualWhere
      ? JSON.parse(JSON.stringify(residualWhere))
      : undefined,
    group_expressions: keepOriginalSelect ? clonedNode.group_expressions : [],
    group_sets: keepOriginalSelect ? clonedNode.group_sets : [],
    having: hasResidualHaving
      ? JSON.parse(JSON.stringify(residualHaving))
      : null,
    qualify: clonedNode.qualify || null,
    sample: null,
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
