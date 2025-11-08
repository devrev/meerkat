import { MeerkatQueryFilter, QueryFilter } from '../types/cube-types';

/**
 * Identifies filters that are redundant between base query and projection query
 * Returns filters that should be kept in the outer query
 */
export const deduplicateFilters = (
  projectionFilters: MeerkatQueryFilter[],
  baseQuerySQL: string
): MeerkatQueryFilter[] => {
  const filterIsInBaseQuery = (filter: QueryFilter): boolean => {
    const { member, operator, values } = filter;

    // Extract member name without table prefix
    const memberName = member.split('.').pop();
    if (!memberName) return false;

    // Check if this exact filter exists in base query
    if (operator === 'in' && values && Array.isArray(values)) {
      // For IN operators, check if the base query has the same filter
      const inPattern = new RegExp(`${memberName}\\s+IN\\s*\\([^)]*\\)`, 'i');
      return inPattern.test(baseQuerySQL);
    }

    if (operator === 'equals' && values && values.length > 0) {
      const equalsPattern = new RegExp(
        `${memberName}\\s*=\\s*'${values[0]}'`,
        'i'
      );
      return equalsPattern.test(baseQuerySQL);
    }

    return false;
  };

  const deduplicateFilter = (
    filter: MeerkatQueryFilter
  ): MeerkatQueryFilter | null => {
    if ('member' in filter) {
      return filterIsInBaseQuery(filter) ? null : filter;
    }

    if ('and' in filter) {
      const dedupedFilters = filter.and
        .map(deduplicateFilter)
        .filter(Boolean) as MeerkatQueryFilter[];
      return dedupedFilters.length > 0 ? { and: dedupedFilters } : null;
    }

    if ('or' in filter) {
      const dedupedFilters = filter.or
        .map(deduplicateFilter)
        .filter(Boolean) as MeerkatQueryFilter[];
      return dedupedFilters.length > 0 ? { or: dedupedFilters } : null;
    }

    return filter;
  };

  return projectionFilters
    .map(deduplicateFilter)
    .filter(Boolean) as MeerkatQueryFilter[];
};
