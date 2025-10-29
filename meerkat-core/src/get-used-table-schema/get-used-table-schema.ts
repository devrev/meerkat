import { traverseMeerkatQueryFilter } from '../filter-params/filter-params-ast';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { Member, Query, QueryFilter, TableSchema } from '../types/cube-types';

export const getUsedTableSchema = (
  tableSchema: TableSchema[],
  cubeQuery: Query
): TableSchema[] => {
  if (cubeQuery.joinPaths?.length) {
    return tableSchema;
  }

  const getTableFromMember = (member: Member): string => {
    return splitIntoDataSourceAndFields(member)[0];
  };

  // Get all tables mentioned in filters
  const usedTables = new Set<string>();

  traverseMeerkatQueryFilter(cubeQuery.filters || [], (filter: QueryFilter) => {
    usedTables.add(getTableFromMember(filter.member));
  });

  // Add tables from measures
  if (cubeQuery.measures && cubeQuery.measures.length > 0) {
    cubeQuery.measures.forEach((measure) => {
      if (measure === '*') {
        tableSchema.forEach((schema) => {
          usedTables.add(schema.name);
        });
      } else {
        usedTables.add(getTableFromMember(measure));
      }
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

  // Filter table schema to only include used tables
  const filteredSchema = tableSchema.filter((schema) =>
    usedTables.has(schema.name)
  );
  return filteredSchema;
};
