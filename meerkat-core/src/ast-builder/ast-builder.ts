import { cubeFilterToDuckdbAST } from '../cube-filter-transformer/factory';
import { cubeDimensionToGroupByAST } from '../cube-group-by-transformer/cube-group-by-transformer';
import { cubeLimitOffsetToAST } from '../cube-limit-offset-transformer/cube-limit-offset-transformer';
import { cubeOrderByToAST } from '../cube-order-by-transformer/cube-order-by-transformer';
import { QueryFiltersWithInfo } from '../cube-to-duckdb/cube-filter-to-duckdb';
import { FilterType, Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { cubeFiltersEnrichment } from '../utils/cube-filter-enrichment';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';
import { modifyLeafMeerkatFilter } from '../utils/modify-meerkat-filter';

const getSeparatedWhereAndHavingFilters = (query: Query, queryFiltersWithInfo: QueryFiltersWithInfo) => {
  const having: QueryFiltersWithInfo = [];
  const where: QueryFiltersWithInfo = [];
  
  queryFiltersWithInfo.forEach((filter) => {
    if ('member' in filter) {
      const isMeasure = query.measures.includes(filter.member)
      if (isMeasure) {
        having.push({...filter, member:  memberKeyToSafeKey(filter.member)});
      } else {
        where.push(filter);
      }
    }
    const andFilters = 'and' in filter ? getSeparatedWhereAndHavingFilters(query, filter.and) : undefined;
    const orFilters = 'or' in filter ? getSeparatedWhereAndHavingFilters(query, filter.or) : undefined;
    
    if (andFilters?.having || orFilters?.having) {
      having.push({
        ...(andFilters?.having ? { and: andFilters.having }: {}),
        ...(orFilters?.having ? { or: orFilters.having }: {}),
      })
    }
    if (andFilters?.where || orFilters?.having) {
      where.push({
        ...(andFilters?.where ? { and: andFilters.where }: {}),
        ...(orFilters?.where ? { or: orFilters.where }: {}),
      })
    }
  })

  return {
    having,
    where
  }
}


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

    /*
    * If the type of filter is set to base filter where 
    */
    const finalFilters = options?.filterType === 'BASE_FILTER' ? queryFiltersWithInfo : modifyLeafMeerkatFilter(queryFiltersWithInfo, (item) => {
      return {
        ...item,
        member: item.member.split('.').join('__')
      };
    }) as QueryFiltersWithInfo; 

    const { having, where } = getSeparatedWhereAndHavingFilters(query, finalFilters)

    const duckdbWhereClause = cubeFilterToDuckdbAST(
      where,
      baseAST
    );

    const duckdbHavingClause = cubeFilterToDuckdbAST(
      having,
      baseAST
    );
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.where_clause = duckdbWhereClause;
    node.having = duckdbHavingClause
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
