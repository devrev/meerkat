import { cubeFilterToDuckdbAST } from '../cube-filter-transformer/factory';
import { cubeDimensionToGroupByAST } from '../cube-group-by-transformer/cube-group-by-transformer';
import { cubeLimitOffsetToAST } from '../cube-limit-offset-transformer/cube-limit-offset-transformer';
import { cubeOrderByToAST } from '../cube-order-by-transformer/cube-order-by-transformer';
import { FilterType, MeerkatFilter, Query, QueryFilter } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { cubeFiltersEnrichment } from '../utils/cube-filter-enrichment';

const modifyLeafMeerkatFilter = <T>(filters: MeerkatFilter, cb: (arg: QueryFilter) => T)  => {
  if (!filters) return filters;
  const modifiedFilters: T[] | { and?: T[], or?: T[] } = filters.map((item) => {
    if ('and' in item) {
      return {
        and: modifyLeafMeerkatFilter(item.and, cb)
      }
    } 
    if ('or' in item) {
      return {
        or: modifyLeafMeerkatFilter(item.or, cb)
      }
    } 
    if ('member' in item) {
      return cb(item)
    }
  })
  return modifiedFilters
};

export const cubeToDuckdbAST = (query: Query, tableSchema: TableSchema, options?: { filterType: FilterType }
) => {
  /**
   * Obviously, if no table schema was found, return null.
   */
  if (!tableSchema) {
    return null;
  }

  const baseAST = getBaseAST();
  const node = baseAST.node as SelectNode;

  if (query.filters && query.filters.length > 0) {
    /**
     * Make a copy of the query filters and enrich them with the table schema.
     */
    const queryFiltersWithInfo = cubeFiltersEnrichment(
      JSON.parse(JSON.stringify(query.filters)),
      tableSchema
    );

    if (!queryFiltersWithInfo) {
      return null;
    }

    const finalFilters = options?.filterType === 'BASE_FILTER' ? modifyLeafMeerkatFilter(queryFiltersWithInfo, (item) => {
      return {
        ...item,
        member: item.member.split('__').join('.')
      }
    }):  queryFiltersWithInfo; 

    const duckdbWhereClause = cubeFilterToDuckdbAST(
      finalFilters,
      baseAST
    );
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.where_clause = duckdbWhereClause;
  }

  if (query.dimensions && query.dimensions?.length > 0) {
    node.group_expressions = cubeDimensionToGroupByAST(query.dimensions);
    const groupSets = [];
    /**
     * We only support one group set for now.
     */
    for (let i = 0; i < node.group_expressions.length; i++) {
      groupSets.push(i);
    }
    node.group_sets = [groupSets];
  }
  node.modifiers = [];
  if (query.order) {
    node.modifiers.push(cubeOrderByToAST(query.order));
  }
  if (query.limit || query.offset) {
    // Type assertion is needed here because the AST is not typed correctly.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.modifiers.push(cubeLimitOffsetToAST(query.limit, query.offset));
  }

  return baseAST;
};
