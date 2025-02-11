import {
  isJoinNode,
  MeerkatQueryFilter,
  Member,
  Query,
  TableSchema,
} from '../types/cube-types';

export const getUsedTableSchema = (
  tableSchema: TableSchema[],
  cubeQuery: Query
): TableSchema[] => {
  const getTableFromMember = (member: Member): string => {
    return member.toString().split('.')[0];
  };

  const getTablesFromFilter = (filter: MeerkatQueryFilter): Set<string> => {
    const tables = new Set<string>();
    if ('and' in filter) {
      filter.and.forEach((filtr) => {
        if ('or' in filtr) {
          filtr.or.forEach((orFilter) => {
            const orTables = getTablesFromFilter(orFilter);
            orTables.forEach((table) => tables.add(table));
          });
        } else {
          tables.add(getTableFromMember(filtr.member));
        }
      });
    } else if ('or' in filter) {
      filter.or.forEach((filtr) => {
        const orTables = getTablesFromFilter(filtr);
        orTables.forEach((table) => tables.add(table));
      });
    } else {
      tables.add(getTableFromMember(filter.member));
    }
    return tables;
  };

  // Get all tables mentioned in filters
  const usedTables = new Set<string>();

  // Add tables from filters
  if (cubeQuery.filters && cubeQuery.filters.length > 0) {
    cubeQuery.filters.forEach((filter) => {
      const filterTables = getTablesFromFilter(filter);
      filterTables.forEach((table) => usedTables.add(table));
    });
  }

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
          usedTables.add(node.right);
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
