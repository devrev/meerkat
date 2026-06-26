import { getNamespacedKey, memberKeyToSafeKey } from '../../member-formatters';
import { Dimension, TableSchema } from '../../types/cube-types/table';
import { findInDimensionSchemas } from '../../utils/find-in-table-schema';
import { ResolutionConfig } from '../types';

/**
 * Builds the qualified alias under which a lookup column is projected.
 * The alias is the table-qualified safe key (`<config>__<column>`), which is
 * globally unique per resolution config — so even when two different lookup
 * tables expose a column with the same raw name (e.g. `deprecated_created_by_agent_id`
 * on both `dim_devu` and `dim_group`), the projected names never collide.
 */
const qualifiedLookupAlias = (configName: string, column: string): string =>
  memberKeyToSafeKey(getNamespacedKey(configName, column));

/**
 * Rewrites a lookup table's SQL so it projects every declared dimension (plus
 * the join column) under a table-qualified alias instead of leaking its raw
 * physical columns.
 *
 * The resolution pipeline joins each lookup as `SELECT <named cols>, * FROM
 * (<lookupSql>)`. The trailing bare `*` re-exposes every raw column of the
 * lookup — including undeclared passthrough columns that several `dim_*` tables
 * share (`deprecated_created_by_agent_id`, etc.). When two lookups are joined,
 * those shared raw names collide and DuckDB's binder rejects the export with
 * `Binder Error: Ambiguous reference to column name` (ISS-301213 / TKT-65015).
 *
 * By projecting each column as `<expr> AS "<config>__<column>"`, the lookup
 * subquery only ever emits uniquely-named columns, so the bare `*` is harmless.
 *
 * Note: declared measures are intentionally not projected here. Resolution
 * lookups are never aggregated (the resolution schema always carries
 * `measures: []`), and a measure's SQL is an aggregate expression that is
 * invalid in this flat, non-grouped projection.
 */
const buildQualifiedLookupSql = (
  sourceSchema: TableSchema,
  configName: string,
  joinColumn: string
): string => {
  const wrapAlias = memberKeyToSafeKey(configName);
  const projections: string[] = [];
  const projectedColumns = new Set<string>();

  const addProjection = (column: string) => {
    if (projectedColumns.has(column)) {
      return;
    }
    projectedColumns.add(column);
    // Reference the raw column off the wrapped subquery alias (the inner
    // `(sourceSchema.sql) AS wrapAlias` exposes each declared column by its
    // physical name), then re-alias it to a globally-unique qualified key.
    projections.push(
      `${wrapAlias}."${column}" AS "${qualifiedLookupAlias(configName, column)}"`
    );
  };

  sourceSchema.dimensions.forEach((dimension) => {
    addProjection(dimension.name);
  });

  // The join column may not be a declared dimension, but the JOIN needs it.
  addProjection(joinColumn);

  return `SELECT ${projections.join(', ')} FROM (${sourceSchema.sql}) AS ${wrapAlias}`;
};

export const generateResolutionSchemas = (config: ResolutionConfig) => {
  const resolutionSchemas: TableSchema[] = [];
  config.columnConfigs.forEach((colConfig) => {
    const tableSchema = config.tableSchemas.find(
      (ts) => ts.name === colConfig.source
    );
    if (!tableSchema) {
      throw new Error(`Table schema not found for ${colConfig.source}`);
    }

    const baseName = memberKeyToSafeKey(colConfig.name);

    // Qualify every column the lookup projects so its bare `*` can never expose
    // a raw column name that clashes with another lookup table (ISS-301213).
    const qualifiedSql = buildQualifiedLookupSql(
      tableSchema,
      colConfig.name,
      colConfig.joinColumn
    );

    // For each column that needs to be resolved, create a copy of the relevant table schema.
    // We use the name of the column in the base query as the table schema name
    // to avoid conflicts.
    const resolutionSchema: TableSchema = {
      name: baseName,
      sql: qualifiedSql,
      measures: [],
      dimensions: colConfig.resolutionColumns.map((col) => {
        const dimension = findInDimensionSchemas(
          getNamespacedKey(colConfig.source, col),
          config.tableSchemas
        );
        if (!dimension) {
          throw new Error(`Dimension not found: ${col}`);
        }
        const dimensionDef: Dimension = {
          // Need to create a new name due to limitations with how
          // CubeToSql handles duplicate dimension names between different sources.
          name: memberKeyToSafeKey(getNamespacedKey(colConfig.name, col)),
          // Reference the qualified alias projected by buildQualifiedLookupSql,
          // not the raw column name (which is no longer exposed).
          sql: `${baseName}."${qualifiedLookupAlias(colConfig.name, col)}"`,
          type: dimension.type,
          alias: memberKeyToSafeKey(getNamespacedKey(colConfig.name, col)),
        };
        return dimensionDef;
      }),
    };

    resolutionSchemas.push(resolutionSchema);
  });

  return resolutionSchemas;
};

/**
 * The qualified alias under which a lookup's join column is projected by
 * {@link buildQualifiedLookupSql}. Exported so the join ON clause can reference
 * the same alias.
 */
export const resolutionJoinColumnAlias = (
  configName: string,
  joinColumn: string
): string => qualifiedLookupAlias(configName, joinColumn);
