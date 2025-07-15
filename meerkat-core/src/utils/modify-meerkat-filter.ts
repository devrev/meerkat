import {
  QueryFiltersWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';

export const modifyLeafMeerkatFilter = <T>(
  filters: QueryFiltersWithInfo,
  callback: (arg: QueryOperatorsWithInfo) => T
): T[] | undefined => {
  /*
   ** This function traverse the QueryFiltersWithInfo JSON, and calls the callback for each leaf type.
   ** This way we need no rewrite the traversal logic again and again.
   ** The return value of the callback will show up the same path in the resulting object as in the original object
   */
  if (!filters) return undefined;
  return filters.map((item) => {
    if ('member' in item) {
      return callback(item);
    } else {
      const andPayload: T[] | undefined =
        'and' in item ? modifyLeafMeerkatFilter(item.and, callback) : undefined;
      const orPayload: T[] | undefined =
        'or' in item ? modifyLeafMeerkatFilter(item.or, callback) : undefined;

      return {
        ...(andPayload ? { and: andPayload } : {}),
        ...(orPayload ? { or: orPayload } : {}),
      } as T;
    }
  });
};
