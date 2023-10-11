import { Query } from './types/cube-types/query';
import { TableSchema } from './types/cube-types/table';

const querySeperator = (query: Query): Query[] => {
  /**
   * Seperate the queries into seperate queries for each table schema.
   * The table name is present as the each measure & filter in the form of: table_name.column_name
   */

  const queries: Query[] = [];

  return queries;
};

const cubeASTBuilder = (query: Query, tableSchemas: TableSchema[]) => {
  /**
   * Build subqueries for each table schema & query.
   */
};
