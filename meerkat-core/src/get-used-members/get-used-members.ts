import { traverseMeerkatQueryFilter } from '../filter-params/filter-params-ast';
import { Member, Query } from '../types/cube-types';

/**
 * Collects every `tableName.memberName` ref the query touches
 * (measures, dimensions, order, filters). Member-level parallel to
 * `getUsedTableSchema`. `joinPaths` excluded — its entries are table
 * names, not member refs.
 */
export const getUsedMembers = (query: Query): Set<Member> => {
  const used = new Set<Member>();

  query.measures.forEach((measure) => used.add(measure));
  query.dimensions?.forEach((dimension) => used.add(dimension));

  if (query.order) {
    Object.keys(query.order).forEach((orderKey) => used.add(orderKey));
  }

  traverseMeerkatQueryFilter(query.filters || [], (filter) => {
    used.add(filter.member);
  });

  return used;
};
