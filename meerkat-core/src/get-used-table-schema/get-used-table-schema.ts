import { traverseMeerkatQueryFilter } from '../filter-params/filter-params-ast';
import {
  Member,
  Query,
  QueryFilter,
  TableSchema,
  isJoinNode,
} from '../types/cube-types';

export const getUsedTableSchema = (
  tableSchema: TableSchema[],
  cubeQuery: Query
): TableSchema[] => {
  const getTableFromMember = (member: Member): string => {
    return member.toString().split('.')[0];
  };

  // Get all tables mentioned in filters
  const usedTables = new Set<string>();

  traverseMeerkatQueryFilter(cubeQuery.filters || [], (filter: QueryFilter) => {
    usedTables.add(getTableFromMember(filter.member));
  });

  // Add tables from measures
  if (cubeQuery.measures && cubeQuery.measures.length > 0) {
    cubeQuery.measures.forEach((measure) => {
      usedTables.add(getTableFromMember(measure));
    });
  }

  // Add tables from dimensions
  if (cubeQuery.dimensions && cubeQuery.dimensions.length > 0) {
    cubeQuery.dimensions.forEach((dimension) => {
      usedTables.add(getTableFromMember(dimension));
    });
  }

  // Add tables from order
  if (cubeQuery.order) {
    Object.keys(cubeQuery.order).forEach((orderKey) => {
      usedTables.add(getTableFromMember(orderKey));
    });
  }

  // Add tables from joinPaths
  if (cubeQuery.joinPaths && cubeQuery.joinPaths.length > 0) {
    cubeQuery.joinPaths.forEach((joinPath) => {
      joinPath.forEach((node) => {
        usedTables.add(getTableFromMember(node.left));
        if (isJoinNode(node)) {
          usedTables.add(getTableFromMember(node.right));
        }
      });
    });
  }

  // Filter table schema to only include used tables
  const filteredSchema = tableSchema.filter((schema) =>
    usedTables.has(schema.name)
  );
  return filteredSchema;
};
