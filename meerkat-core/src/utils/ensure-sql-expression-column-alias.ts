import {
  OrderByNode,
  ParsedExpression,
  QueryNode,
  QueryNodeType,
  ResultModifier,
} from '../types/duckdb-serialization-types';
import {
  isColumnRefExpression,
  isConjunctionExpression,
  isLambdaExpression,
  isOperatorExpression,
  isSelectNode,
  isSubqueryExpression,
} from '../types/utils';
import {
  GetQueryOutput,
  parseExpressions,
  serializeExpressions,
} from './duckdb-ast-parse-serialize';
import { getChildExpressions } from './get-child-expressions';

export interface EnsureColumnAliasBatchItem<TContext = unknown> {
  sql: string;
  tableName: string;
  context?: TContext;
}

export interface EnsureColumnAliasBatchResult<TContext = unknown>
  extends EnsureColumnAliasBatchItem<TContext> {
  didChange: boolean;
}

export interface EnsureColumnAliasBatchParams<TContext = unknown> {
  items: EnsureColumnAliasBatchItem<TContext>[];
  executeQuery: GetQueryOutput;
}

const ALIASABLE_IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

const shouldEnsureColumnRefAlias = (
  columnNames: string[],
  scopedIdentifiers: Set<string>
): boolean => {
  if (columnNames.length !== 1) {
    return false;
  }

  const [columnName] = columnNames;

  if (!columnName) {
    return false;
  }

  if (/\s/.test(columnName)) {
    return false;
  }

  if (scopedIdentifiers.has(columnName)) {
    return false;
  }

  return ALIASABLE_IDENTIFIER_REGEX.test(columnName);
};

const getLambdaBoundIdentifiers = (
  node: ParsedExpression,
  boundIdentifiers: Set<string> = new Set()
): Set<string> => {
  if (isColumnRefExpression(node) && node.column_names.length === 1) {
    boundIdentifiers.add(node.column_names[0]);
    return boundIdentifiers;
  }

  if (isConjunctionExpression(node) || isOperatorExpression(node)) {
    node.children.forEach((child) =>
      getLambdaBoundIdentifiers(child, boundIdentifiers)
    );
  }

  return boundIdentifiers;
};

const ensureOrderByNodesAlias = (
  orders: OrderByNode[],
  scopedIdentifiers: Set<string>,
  tableName?: string
): boolean => {
  return orders.reduce(
    (changed, order) =>
      ensureParsedExpressionAlias(
        order.expression,
        tableName,
        scopedIdentifiers
      ) || changed,
    false
  );
};

const isOrderModifier = (
  modifier: ResultModifier
): modifier is ResultModifier & { orders: OrderByNode[] } => {
  return modifier.type === 'ORDER_MODIFIER';
};

const isDistinctModifier = (
  modifier: ResultModifier
): modifier is ResultModifier & { distinct_on_targets: ParsedExpression[] } => {
  return modifier.type === 'DISTINCT_MODIFIER';
};

const isLimitLikeModifier = (
  modifier: ResultModifier
): modifier is ResultModifier & {
  limit?: ParsedExpression;
  offset?: ParsedExpression;
} => {
  return (
    modifier.type === 'LIMIT_MODIFIER' ||
    modifier.type === 'LIMIT_PERCENT_MODIFIER'
  );
};

const ensureQueryNodeAlias = (
  node: QueryNode,
  tableName?: string,
  scopedIdentifiers: Set<string> = new Set()
): boolean => {
  if (isSelectNode(node)) {
    let changed = false;

    node.select_list.forEach((expression) => {
      changed =
        ensureParsedExpressionAlias(
          expression,
          tableName,
          scopedIdentifiers
        ) || changed;
    });
    changed =
      (node.where_clause
        ? ensureParsedExpressionAlias(
            node.where_clause,
            tableName,
            scopedIdentifiers
          )
        : false) || changed;
    node.group_expressions.forEach((expression) => {
      changed =
        ensureParsedExpressionAlias(
          expression,
          tableName,
          scopedIdentifiers
        ) || changed;
    });
    changed =
      (node.having
        ? ensureParsedExpressionAlias(
            node.having,
            tableName,
            scopedIdentifiers
          )
        : false) || changed;
    changed =
      (node.qualify
        ? ensureParsedExpressionAlias(
            node.qualify,
            tableName,
            scopedIdentifiers
          )
        : false) || changed;

    node.modifiers.forEach((modifier) => {
      if (isOrderModifier(modifier)) {
        changed =
          ensureOrderByNodesAlias(
            modifier.orders,
            scopedIdentifiers,
            tableName
          ) || changed;
      }

      if (isDistinctModifier(modifier)) {
        modifier.distinct_on_targets.forEach(
          (target) =>
            (changed =
              ensureParsedExpressionAlias(
                target,
                tableName,
                scopedIdentifiers
              ) || changed)
        );
      }

      if (isLimitLikeModifier(modifier)) {
        changed =
          (modifier.limit
            ? ensureParsedExpressionAlias(
                modifier.limit,
                tableName,
                scopedIdentifiers
              )
            : false) || changed;
        changed =
          (modifier.offset
            ? ensureParsedExpressionAlias(
                modifier.offset,
                tableName,
                scopedIdentifiers
              )
            : false) || changed;
      }
    });

    return changed;
  }

  if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(node.left, tableName, scopedIdentifiers) || changed;
    changed =
      ensureQueryNodeAlias(node.right, tableName, scopedIdentifiers) || changed;
    return changed;
  }

  if (node.type === QueryNodeType.RECURSIVE_CTE_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(node.left, tableName, scopedIdentifiers) || changed;
    changed =
      ensureQueryNodeAlias(node.right, tableName, scopedIdentifiers) || changed;
    return changed;
  }

  if (node.type === QueryNodeType.CTE_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(node.query, tableName, scopedIdentifiers) || changed;
    changed =
      ensureQueryNodeAlias(node.child, tableName, scopedIdentifiers) || changed;
    return changed;
  }

  return false;
};

const ensureParsedExpressionAlias = (
  node: ParsedExpression,
  tableName?: string,
  scopedIdentifiers: Set<string> = new Set()
): boolean => {
  if (!node || !tableName) {
    return false;
  }

  if (isColumnRefExpression(node)) {
    if (shouldEnsureColumnRefAlias(node.column_names, scopedIdentifiers)) {
      node.column_names = [tableName, node.column_names[0]];
      return true;
    }
    return false;
  }

  if (isLambdaExpression(node)) {
    const lambdaScopedIdentifiers = new Set(scopedIdentifiers);
    getLambdaBoundIdentifiers(node.lhs).forEach((identifier) =>
      lambdaScopedIdentifiers.add(identifier)
    );

    return node.expr
      ? ensureParsedExpressionAlias(
          node.expr,
          tableName,
          lambdaScopedIdentifiers
        )
      : false;
  }

  if (isSubqueryExpression(node)) {
    let changed = ensureQueryNodeAlias(
      node.subquery.node,
      tableName,
      scopedIdentifiers
    );
    if (node.child) {
      changed =
        ensureParsedExpressionAlias(
          node.child,
          tableName,
          scopedIdentifiers
        ) || changed;
    }
    return changed;
  }

  return getChildExpressions(node).reduce(
    (changed, child) =>
      ensureParsedExpressionAlias(child, tableName, scopedIdentifiers) ||
      changed,
    false
  );
};

export const ensureColumnAliasBatch = async <TContext = unknown>({
  items,
  executeQuery,
}: EnsureColumnAliasBatchParams<TContext>): Promise<
  EnsureColumnAliasBatchResult<TContext>[]
> => {
  if (items.length === 0) {
    return [];
  }

  const parsedExpressions = await parseExpressions(
    items.map((item) => item.sql),
    executeQuery
  );

  if (parsedExpressions.length !== items.length) {
    throw new Error(
      `Expected ${items.length} parsed expressions, received ${parsedExpressions.length}`
    );
  }

  const results: EnsureColumnAliasBatchResult<TContext>[] = items.map(
    (item) => ({
      ...item,
      sql: item.sql,
      didChange: false,
    })
  );

  const changedResultIndexes: number[] = [];

  parsedExpressions.forEach((parsedExpression, index) => {
    const didChange = ensureParsedExpressionAlias(
      parsedExpression,
      items[index].tableName
    );
    if (!didChange) {
      return;
    }

    results[index].didChange = true;
    changedResultIndexes.push(index);
  });

  if (changedResultIndexes.length > 0) {
    const serializedSqls = await serializeExpressions(
      parsedExpressions,
      executeQuery
    );
    const expectedSerializedCount = items.length;

    if (serializedSqls.length !== expectedSerializedCount) {
      throw new Error(
        `Expected ${expectedSerializedCount} serialized expressions, received ${serializedSqls.length}`
      );
    }

    changedResultIndexes.forEach((resultIndex) => {
      const serializedSql = serializedSqls[resultIndex];
      if (!serializedSql) {
        throw new Error('Missing serialized SQL for batched expression');
      }

      results[resultIndex].sql = serializedSql;
    });
  }

  return results;
};
