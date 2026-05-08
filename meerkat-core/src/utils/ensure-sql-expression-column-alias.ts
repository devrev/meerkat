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
  /**
   * Names of all tables in the current schema batch. Used to distinguish a
   * cross-table reference (e.g. `customers.id`) from struct field access
   * (e.g. `stage.stage_id`) on multi-part column references.
   */
  knownTableNames?: Set<string>;
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

const isAliasableIdentifier = (identifier: string | undefined): boolean => {
  if (!identifier) {
    return false;
  }
  if (/\s/.test(identifier)) {
    return false;
  }
  return ALIASABLE_IDENTIFIER_REGEX.test(identifier);
};

const shouldEnsureColumnRefAlias = (
  columnNames: string[],
  scopedIdentifiers: Set<string>,
  tableName: string,
  knownTableNames?: Set<string>
): boolean => {
  if (columnNames.length === 0) {
    return false;
  }

  const [root] = columnNames;

  if (!isAliasableIdentifier(root)) {
    return false;
  }

  if (scopedIdentifiers.has(root)) {
    return false;
  }

  if (columnNames.length === 1) {
    return true;
  }

  if (columnNames.length === 2) {
    if (root === tableName) {
      return false;
    }
    // Without a schema batch, a two-part ref is ambiguous between
    // `table.column` and `struct.field`; stay conservative.
    if (!knownTableNames) {
      return false;
    }
    if (knownTableNames.has(root)) {
      return false;
    }
    return true;
  }

  return false;
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
  tableName?: string,
  knownTableNames?: Set<string>
): boolean => {
  return orders.reduce(
    (changed, order) =>
      ensureParsedExpressionAlias(
        order.expression,
        tableName,
        scopedIdentifiers,
        knownTableNames
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
  scopedIdentifiers: Set<string> = new Set(),
  knownTableNames?: Set<string>
): boolean => {
  if (isSelectNode(node)) {
    let changed = false;

    node.select_list.forEach((expression) => {
      changed =
        ensureParsedExpressionAlias(
          expression,
          tableName,
          scopedIdentifiers,
          knownTableNames
        ) || changed;
    });
    changed =
      (node.where_clause
        ? ensureParsedExpressionAlias(
            node.where_clause,
            tableName,
            scopedIdentifiers,
            knownTableNames
          )
        : false) || changed;
    node.group_expressions.forEach((expression) => {
      changed =
        ensureParsedExpressionAlias(
          expression,
          tableName,
          scopedIdentifiers,
          knownTableNames
        ) || changed;
    });
    changed =
      (node.having
        ? ensureParsedExpressionAlias(
            node.having,
            tableName,
            scopedIdentifiers,
            knownTableNames
          )
        : false) || changed;
    changed =
      (node.qualify
        ? ensureParsedExpressionAlias(
            node.qualify,
            tableName,
            scopedIdentifiers,
            knownTableNames
          )
        : false) || changed;

    node.modifiers.forEach((modifier) => {
      if (isOrderModifier(modifier)) {
        changed =
          ensureOrderByNodesAlias(
            modifier.orders,
            scopedIdentifiers,
            tableName,
            knownTableNames
          ) || changed;
      }

      if (isDistinctModifier(modifier)) {
        modifier.distinct_on_targets.forEach(
          (target) =>
            (changed =
              ensureParsedExpressionAlias(
                target,
                tableName,
                scopedIdentifiers,
                knownTableNames
              ) || changed)
        );
      }

      if (isLimitLikeModifier(modifier)) {
        changed =
          (modifier.limit
            ? ensureParsedExpressionAlias(
                modifier.limit,
                tableName,
                scopedIdentifiers,
                knownTableNames
              )
            : false) || changed;
        changed =
          (modifier.offset
            ? ensureParsedExpressionAlias(
                modifier.offset,
                tableName,
                scopedIdentifiers,
                knownTableNames
              )
            : false) || changed;
      }
    });

    return changed;
  }

  if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(
        node.left,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    changed =
      ensureQueryNodeAlias(
        node.right,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    return changed;
  }

  if (node.type === QueryNodeType.RECURSIVE_CTE_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(
        node.left,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    changed =
      ensureQueryNodeAlias(
        node.right,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    return changed;
  }

  if (node.type === QueryNodeType.CTE_NODE) {
    let changed = false;
    changed =
      ensureQueryNodeAlias(
        node.query,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    changed =
      ensureQueryNodeAlias(
        node.child,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed;
    return changed;
  }

  return false;
};

const ensureParsedExpressionAlias = (
  node: ParsedExpression,
  tableName?: string,
  scopedIdentifiers: Set<string> = new Set(),
  knownTableNames?: Set<string>
): boolean => {
  if (!node || !tableName) {
    return false;
  }

  if (isColumnRefExpression(node)) {
    if (
      shouldEnsureColumnRefAlias(
        node.column_names,
        scopedIdentifiers,
        tableName,
        knownTableNames
      )
    ) {
      node.column_names = [tableName, ...node.column_names];
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
          lambdaScopedIdentifiers,
          knownTableNames
        )
      : false;
  }

  if (isSubqueryExpression(node)) {
    let changed = ensureQueryNodeAlias(
      node.subquery.node,
      tableName,
      scopedIdentifiers,
      knownTableNames
    );
    if (node.child) {
      changed =
        ensureParsedExpressionAlias(
          node.child,
          tableName,
          scopedIdentifiers,
          knownTableNames
        ) || changed;
    }
    return changed;
  }

  return getChildExpressions(node).reduce(
    (changed, child) =>
      ensureParsedExpressionAlias(
        child,
        tableName,
        scopedIdentifiers,
        knownTableNames
      ) || changed,
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
      items[index].tableName,
      new Set(),
      items[index].knownTableNames
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
