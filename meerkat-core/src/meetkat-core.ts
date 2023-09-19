import { Query, TableSchema } from '@devrev/cube-types';
import { SelectNode } from '@devrev/duckdb-serialization-types';
import { cubeFilterToDuckdbAST } from './cube-to-duckdb/cube-filter-to-duckdb';
import { getBaseAST } from './utils/base-ast';
import { cubeFiltersEnrichment } from './utils/cube-filter-enrichment';
import { tableKeyFromMeasuresDimension } from './utils/key-from-measures-dimension';

export class MeerkatCore {
  tableSchemas: Map<string, TableSchema> = new Map<string, TableSchema>();

  addTableSchema(tableSchema: TableSchema) {
    this.tableSchemas.set(tableSchema.cube, tableSchema);
  }

  public cubeToDuckdbAST(query: Query) {
    const tableKey: string | null = tableKeyFromMeasuresDimension(query);
    /**
     * If no table key was found, return null.
     */
    if (!tableKey) {
      return null;
    }

    const tableSchema = this.tableSchemas.get(tableKey);
    /**
     * Obviously, if no table schema was found, return null.
     */
    if (!tableSchema) {
      return null;
    }

    const baseAST = getBaseAST();
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

    const duckdbWhereClause = cubeFilterToDuckdbAST(queryFiltersWithInfo);
    (baseAST.node as SelectNode).where_clause = duckdbWhereClause;

    return baseAST;
  }
}
