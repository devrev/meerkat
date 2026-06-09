import { getAliasForSQL } from '../member-formatters/get-alias';
import { getNamespacedKey } from '../member-formatters/get-namespaced-key';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { Member } from '../types/cube-types/query';
import { Measure, TableSchema } from '../types/cube-types/table';
import { meerkatPlaceholderReplacer } from '../utils/meerkat-placeholder-replacer';

/**
 * Resolve the schema that owns a given member by its namespaced source-table
 * prefix. When a single schema is provided we treat it as the source of truth
 * and skip the lookup — that preserves behavior for single-table queries that
 * may pass an arbitrary table prefix in the member key.
 */
const findSchemaForMember = (
  member: string,
  tableSchemas: TableSchema[]
): { schema: TableSchema; tableName: string } | undefined => {
  const [tableName] = splitIntoDataSourceAndFields(member);
  if (tableSchemas.length === 1) {
    return { schema: tableSchemas[0], tableName };
  }
  const schema = tableSchemas.find((s) => s.name === tableName);
  return schema ? { schema, tableName } : undefined;
};

export const cubeMeasureToSQLSelectString = (
  measures: Member[],
  tableSchemas: TableSchema[]
) => {
  let base = 'SELECT';
  for (let i = 0; i < measures.length; i++) {
    const measure = measures[i];
    if (measure === '*') {
      // `*` is a single-table convenience — preserve original behavior by
      // emitting `<firstSchema>.*`. Multi-table joined schemas should not
      // combine `*` with named measures.
      base += ` ${tableSchemas[0].name}.*`;
      continue;
    }
    const resolved = findSchemaForMember(measure, tableSchemas);
    if (!resolved) {
      continue;
    }
    const { schema: ownerSchema, tableName: tableSchemaName } = resolved;
    const [, measureKeyWithoutTable] = splitIntoDataSourceAndFields(measure);

    const aliasKey = getAliasForSQL(measure, ownerSchema);
    // Look up the measure in its own source-table schema. With multiple
    // joined tables the same measure name (e.g. `id___function__count`) can
    // exist on more than one table, so a flat lookup over the merged schema
    // would pick the wrong one. Resolving the owner schema by the member
    // key's source-table prefix avoids the collision.
    const measureSchema = ownerSchema.measures.find(
      (m) => m.name === measureKeyWithoutTable
    );
    if (!measureSchema) {
      continue;
    }
    if (i > 0) {
      base += ', ';
    }

    let meerkatReplacedSqlString = meerkatPlaceholderReplacer(
      measureSchema.sql,
      tableSchemaName,
      ownerSchema
    );

    /**
     * Here we extract the columns used in the measure and replace them with the safeKey.
     * We need to do this because the columns used in the measure are not directly available in the joined table.
     * Thus we need to project them and use them in the join.
     */

    const columnsUsedInMeasure = getColumnsFromSQL(
      meerkatReplacedSqlString,
      tableSchemaName
    );

    //Replace all the columnsUsedInMeasure with safeKey
    columnsUsedInMeasure?.forEach((measureKey) => {
      const [, column] = splitIntoDataSourceAndFields(measureKey);
      const memberKey = getNamespacedKey(tableSchemaName, column);
      const columnKey = getAliasForSQL(memberKey, ownerSchema);
      meerkatReplacedSqlString = meerkatReplacedSqlString.replace(
        memberKey,
        columnKey
      );
    });

    base += ` ${meerkatReplacedSqlString} AS ${aliasKey} `;
  }
  return base;
};

const addDimensionToSQLProjection = (
  dimensions: Member[],
  selectString: string,
  tableSchemas: TableSchema[]
) => {
  if (dimensions.length === 0) {
    return selectString;
  }
  let newSelectString = selectString;
  for (let i = 0; i < dimensions.length; i++) {
    const dimension = dimensions[i];
    const resolved = findSchemaForMember(dimension, tableSchemas);
    if (!resolved) {
      continue;
    }
    const { schema: ownerSchema } = resolved;
    const [, dimensionKeyWithoutTable] =
      splitIntoDataSourceAndFields(dimension);
    // See comment in `cubeMeasureToSQLSelectString` — resolve the dimension
    // in its own source-table schema so duplicate names across joined tables
    // don't collide.
    const dimensionSchema = ownerSchema.dimensions.find(
      (m) => m.name === dimensionKeyWithoutTable
    );
    const aliasKey = getAliasForSQL(dimension, ownerSchema);

    if (!dimensionSchema) {
      continue;
    }
    if (i > 0) {
      newSelectString += ',';
    }
    // since alias key is expected to have been unfurled in the base query, we can just use it as is.
    newSelectString += `  ${aliasKey}`;
  }
  return newSelectString;
};

export const getSelectReplacedSql = (sql: string, selectString: string) => {
  /*
   ** Replaces the select portion of a SQL string with the selectString passed.
   */
  const selectRegex = /SELECT\s\*/;
  const match = sql.match(selectRegex);
  if (!match) {
    return sql;
  }
  const selectIndex = match.index;
  if (selectIndex === undefined) {
    throw new Error('SELECT * not found in SQL string');
  }
  const selectLength = match[0].length;
  const beforeSelect = sql.substring(0, selectIndex);
  const afterSelect = sql.substring(selectIndex + selectLength);
  return `${beforeSelect}${selectString}${afterSelect}`;
};

/**
 * Get all the columns used in the measures.
 * This is used for extracting the columns used in the measures needed for the projection.
 * Example: The joins implementation uses this to get the columns used in the measures to join the tables.
 * like the SQL for the measure is `SUM(table.total)` and the table name is `table`, then the column used is `total`
 * table cannot be used directly here because the joined table would have column name ambiguity.
 * Thus these columns are projected and directly used in the join.
 */
export const getAllColumnUsedInMeasures = (
  measures: Measure[],
  tableSchema: TableSchema
) => {
  let columns: string[] = [];
  measures.forEach((measure) => {
    const columnMatch = getColumnsFromSQL(measure.sql, tableSchema.name);
    if (columnMatch && columnMatch.length > 0) {
      columns = [...columns, ...columnMatch];
    }
  });
  // Remove duplicates
  return [...new Set(columns)];
};

const getColumnsFromSQL = (sql: string, tableName: string) => {
  // Match table.column patterns that are NOT preceded by a quote character
  // This prevents matching already-quoted aliases like "orders.column"
  const regex = new RegExp(
    `(?<!")\\b(${tableName}\\.[a-zA-Z0-9_]+)\\b(?!")`,
    'g'
  );
  const columnMatch = sql.match(regex);
  return columnMatch;
};

/**
 * Replace the first SELECT * from the sqlToReplace with the cube measure.
 *
 * `tableSchemas` is the list of *original* per-table schemas (pre-merge), so
 * member lookups can be scoped to the table named in each member key. This
 * avoids collisions when joined tables expose measures/dimensions with
 * identical names.
 */
export const applyProjectionToSQLQuery = (
  dimensions: Member[],
  measures: Member[],
  tableSchemas: TableSchema[],
  sqlToReplace: string
) => {
  let measureSelectString = cubeMeasureToSQLSelectString(
    measures,
    tableSchemas
  );

  if (measures.length > 0 && dimensions.length > 0) {
    measureSelectString += ', ';
  }
  const selectString = addDimensionToSQLProjection(
    dimensions,
    measureSelectString,
    tableSchemas
  );

  return getSelectReplacedSql(sqlToReplace, selectString);
};
