import {
  astDeserializerQuery,
  deserializeQuery,
} from '../ast-deserializer/ast-deserializer';
import { astSerializerQuery } from '../ast-serializer/ast-serializer';
import { getSelectNodes } from '../ast-validator';
import { sanitizeStringValue } from '../member-formatters/sanitize-value';
import {
  AggregateHandling,
  ParsedExpression,
  QueryNodeType,
  TableReferenceType,
} from '../types/duckdb-serialization-types';

export type GetQueryOutput = (
  query: string
) => Promise<Record<string, string>[]>;

const stripSelectWrapper = (query: string): string => {
  const normalized = query.trim().replace(/;$/, '');

  if (!/^SELECT\s+/i.test(normalized)) {
    throw new Error(`Expected SELECT wrapper, received: ${normalized}`);
  }

  return normalized.replace(/^SELECT\s+/i, '').trim();
};

const BATCH_SERIALIZE_ALIAS_PREFIX = '__meerkat_batch_expr_';

const getBatchSerializeAlias = (index: number) => {
  return `${BATCH_SERIALIZE_ALIAS_PREFIX}${index}__`;
};

const splitBatchSerializedExpressions = (
  sql: string,
  expressionCount: number
): string[] => {
  const expressionSql = stripSelectWrapper(sql);
  let cursor = 0;
  const expressions: string[] = [];

  for (let index = 0; index < expressionCount; index += 1) {
    const aliasToken = ` AS ${getBatchSerializeAlias(index)}`;
    const aliasIndex = expressionSql.indexOf(aliasToken, cursor);

    if (aliasIndex === -1) {
      throw new Error(
        `Failed to locate batch serialization alias ${getBatchSerializeAlias(
          index
        )}`
      );
    }

    expressions.push(expressionSql.slice(cursor, aliasIndex).trim());
    cursor = aliasIndex + aliasToken.length;

    if (index < expressionCount - 1) {
      if (expressionSql[cursor] !== ',') {
        throw new Error(
          `Expected comma after batch serialization alias ${getBatchSerializeAlias(
            index
          )}`
        );
      }

      cursor += 1;

      while (expressionSql[cursor] === ' ') {
        cursor += 1;
      }
    }
  }

  return expressions;
};

const stripQueryLocationInPlace = (root: unknown): void => {
  const stack: unknown[] = [root];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') {
      continue;
    }

    if (Array.isArray(current)) {
      current.forEach((item) => stack.push(item));
      continue;
    }

    const record = current as Record<string, unknown>;
    if (Object.prototype.hasOwnProperty.call(record, 'query_location')) {
      delete record['query_location'];
    }

    Object.values(record).forEach((value) => stack.push(value));
  }
};

export const parseExpressions = async (
  sqls: string[],
  executeQuery: GetQueryOutput
): Promise<ParsedExpression[]> => {
  if (sqls.length === 0) {
    return [];
  }

  const selectSql = `SELECT ${sqls.join(', ')}`;
  const rows = await executeQuery(
    astSerializerQuery(sanitizeStringValue(selectSql))
  );
  const parsedSerialization = JSON.parse(deserializeQuery(rows));

  stripQueryLocationInPlace(parsedSerialization);

  const parsedExpressions = getSelectNodes(parsedSerialization);
  if (parsedExpressions.length !== sqls.length) {
    throw new Error(
      `Expected ${sqls.length} parsed expressions, received ${parsedExpressions.length}`
    );
  }

  return parsedExpressions;
};

export const serializeExpressions = async (
  expressions: ParsedExpression[],
  executeQuery: GetQueryOutput
): Promise<string[]> => {
  if (expressions.length === 0) return [];

  expressions.forEach((expr, i) => {
    expr.alias = getBatchSerializeAlias(i);
  });

  const statement = {
    node: {
      type: QueryNodeType.SELECT_NODE as const,
      modifiers: [],
      cte_map: { map: [] },
      select_list: expressions,
      from_table: {
        type: TableReferenceType.EMPTY as const,
        alias: '',
        sample: null,
      },
      group_expressions: [],
      group_sets: [],
      aggregate_handling: AggregateHandling.STANDARD_HANDLING,
      having: null,
      sample: null,
      qualify: null,
    },
  };

  const rows = await executeQuery(astDeserializerQuery(statement));
  const deserializedSql = deserializeQuery(rows);
  return splitBatchSerializedExpressions(deserializedSql, expressions.length);
};
