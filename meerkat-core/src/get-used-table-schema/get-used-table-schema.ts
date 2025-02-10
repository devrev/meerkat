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
  if (!cubeQuery.filters || cubeQuery.filters.length === 0) {
    return tableSchema;
  }

  const getTableFromMember = (member: Member): string => {
    return member.toString().split('.')[0];
  };

  const getTablesFromFilter = (filter: MeerkatQueryFilter): Set<string> => {
    const tables = new Set<string>();

    if ('and' in filter) {
      filter.and.forEach((f) => {
        if ('or' in f) {
          f.or.forEach((orFilter) => {
            const orTables = getTablesFromFilter(orFilter);
            orTables.forEach((t) => tables.add(t));
          });
        } else {
          tables.add(getTableFromMember(f.member));
        }
      });
    } else if ('or' in filter) {
      filter.or.forEach((f) => {
        const orTables = getTablesFromFilter(f);
        orTables.forEach((t) => tables.add(t));
      });
    } else {
      tables.add(getTableFromMember(filter.member));
    }

    return tables;
  };

  // Get all tables mentioned in filters
  const usedTables = new Set<string>();

  // Add tables from filters
  cubeQuery.filters.forEach((filter) => {
    const filterTables = getTablesFromFilter(filter);
    filterTables.forEach((t) => usedTables.add(t));
  });

  // Add tables from measures
  cubeQuery.measures?.forEach((measure) => {
    usedTables.add(getTableFromMember(measure));
  });

  // Add tables from dimensions
  cubeQuery.dimensions?.forEach((dimension) => {
    usedTables.add(getTableFromMember(dimension));
  });

  // Add tables from order
  if (cubeQuery.order) {
    Object.keys(cubeQuery.order).forEach((orderKey) => {
      usedTables.add(getTableFromMember(orderKey));
    });
  }

  // Add tables from joinPaths
  if (cubeQuery.joinPaths) {
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
