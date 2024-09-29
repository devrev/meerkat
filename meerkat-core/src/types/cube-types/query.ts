/**
 * Request type data type.
 */
type RequestType = 'multi';

/**
 * Result type data type.
 */
type ResultType = 'default' | 'compact';

/**
 * API type data type.
 */
type ApiType = 'sql' | 'graphql' | 'rest' | 'ws' | 'stream';

/**
 * Parsed query type data type.
 */
type QueryType = 'regularQuery' | 'compareDateRangeQuery' | 'blendingQuery';

/**
 * String that represent query member type.
 */
type MemberType = 'measures' | 'dimensions' | 'segments';

/**
 * Member identifier. Should satisfy to the following regexp: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/
 */
type Member = string;

/**
 * Datetime member identifier. Should satisfy to the following
 * regexp: /^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(second|minute|hour|day|week|month|year))?$/
 */
type TimeMember = string;

/**
 * Filter operator string.
 */
type FilterOperator =
  | 'equals'
  | 'notEquals'
  | 'contains'
  | 'notContains'
  | 'in'
  | 'notIn'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'set'
  | 'notSet'
  | 'inDateRange'
  | 'notInDateRange'
  | 'onTheDate'
  | 'beforeDate'
  | 'afterDate'
  | 'measureFilter';

/**
 * Time dimension granularity data type.
 */
type QueryTimeDimensionGranularity =
  | 'quarter'
  | 'day'
  | 'month'
  | 'year'
  | 'week'
  | 'hour'
  | 'minute'
  | 'second';

/**
 * Query order data type.
 */
type QueryOrderType = 'asc' | 'desc';

/**
 * ApiScopes data type.
 */
type ApiScopes = 'graphql' | 'meta' | 'data' | 'jobs';

export type FilterType = 'BASE_FILTER' | 'PROJECTION_FILTER';

interface QueryFilter {
  member: Member;
  operator: FilterOperator;
  values?: string[];
}

/**
 * Query 'and'-filters type definition.
 */
type LogicalAndFilter = {
  and: (QueryFilter | { or: (QueryFilter | LogicalAndFilter)[] })[];
};

/**
 * Query 'or'-filters type definition.
 */
type LogicalOrFilter = {
  or: (QueryFilter | LogicalAndFilter)[];
};

/**
 * Query datetime dimention interface.
 */
interface QueryTimeDimension {
  dimension: Member;
  dateRange?: string[] | string;
  granularity?: QueryTimeDimensionGranularity;
}

/**
 * Join Edge data type.
 */

interface JoinNode {
  /**
   * Left node.
   */
  left: Member;

  /**
   * Right node.
   */
  right: Member;

  /**
   * On condition.
   */
  on: string;

  /**
   * Example
   * [
   *  [
   *    {
   *      left:  dim_ticket,
   *      right: dim_user
   *      on: 'created_by_id'
   *    },
   *   {
   *     left : dim_user,
   *     right: dim_user_role,
   *     on: 'role_id'
   *   }
   *  ]
   * ]
   *
   *
   */
}

/**
 * Single node data type.
 * This is the case when there is no join. Just a single node.
 */
interface SingleNode {
  /**
   * Left node.
   */
  left: Member;
}

type JoinPath = [JoinNode | SingleNode, ...JoinNode[]];

export const isJoinNode = (node: JoinNode | SingleNode): node is JoinNode => {
  return 'right' in node;
};

/**
 * Incoming network query data type.
 */

type MeerkatQueryFilter = QueryFilter | LogicalAndFilter | LogicalOrFilter;

interface Query {
  measures: Member[];
  dimensions?: (Member | TimeMember)[];
  filters?: MeerkatQueryFilter[];
  timeDimensions?: QueryTimeDimension[];
  joinPaths?: JoinPath[];
  segments?: Member[];
  limit?: null | number;
  offset?: number;
  total?: boolean;
  totalQuery?: boolean;
  order?: any;
  timezone?: string;
  renewQuery?: boolean;
  ungrouped?: boolean;
  responseFormat?: ResultType;
}

/**
 * Normalized filter interface.
 */
interface NormalizedQueryFilter extends QueryFilter {
  dimension?: Member;
}

/**
 * Normalized query interface.
 */
interface NormalizedQuery extends Query {
  filters?: NormalizedQueryFilter[];
  rowLimit?: null | number;
  order?: [{ id: string; desc: boolean }];
}

export {
  ApiScopes,
  ApiType,
  FilterOperator,
  JoinPath,
  LogicalAndFilter,
  LogicalOrFilter,
  MeerkatQueryFilter,
  Member,
  MemberType,
  NormalizedQuery,
  NormalizedQueryFilter,
  Query,
  QueryFilter,
  QueryOrderType,
  QueryTimeDimension,
  QueryTimeDimensionGranularity,
  QueryType,
  RequestType,
  ResultType,
  TimeMember
};
