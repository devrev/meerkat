import { getNamespacedKey, memberKeyToSafeKey } from '../member-formatters';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { Member } from '../types/cube-types/query';
import { Measure, TableSchema } from '../types/cube-types/table';
import { meerkatPlaceholderReplacer } from '../utils/meerkat-placeholder-replacer';

export const cubeMeasureToSQLSelectString = (
  measures: Member[],
  tableSchema: TableSchema
) => {
  let base = 'SELECT';
  for (let i = 0; i < measures.length; i++) {
    const measure = measures[i];
    if (measure === '*') {
      base += ` ${tableSchema.name}.*`;
      continue;
    }
    const [tableSchemaName, measureKeyWithoutTable] =
      splitIntoDataSourceAndFields(measure);

    // Use safe key internally
    const safeKey = memberKeyToSafeKey(measure);
    const measureSchema = tableSchema.measures.find(
      (m) => m.name === measureKeyWithoutTable
    );
    if (!measureSchema) {
      continue;
    }
    if (i > 0) {
      base += ', ';
    }

    // Note that tableSchemaName is not necessarily the same as tableSchema.name
    // since tableSchema might be the merged tableSchema.
    let meerkatReplacedSqlString = meerkatPlaceholderReplacer(
      measureSchema.sql,
      tableSchemaName
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
      const columnKey = `"${memberKeyToSafeKey(memberKey)}"`;
      meerkatReplacedSqlString = meerkatReplacedSqlString.replace(
        memberKey,
        columnKey
      );
    });

    base += ` ${meerkatReplacedSqlString} AS "${safeKey}" `;
  }
  return base;
};

const addDimensionToSQLProjection = (
  dimensions: Member[],
  selectString: string,
  tableSchema: TableSchema
) => {
  if (dimensions.length === 0) {
    return selectString;
  }
  let newSelectString = selectString;
  for (let i = 0; i < dimensions.length; i++) {
    const dimension = dimensions[i];
    const [, dimensionKeyWithoutTable] =
      splitIntoDataSourceAndFields(dimension);
    const dimensionSchema = tableSchema.dimensions.find(
      (m) => m.name === dimensionKeyWithoutTable
    );
    // Use safe key internally
    const safeKey = `"${memberKeyToSafeKey(dimension)}"`;

    if (!dimensionSchema) {
      continue;
    }
    if (i > 0) {
      newSelectString += ',';
    }
    // Use safe keys in internal queries
    newSelectString += `  ${safeKey}`;
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
  const regex = new RegExp(`(${tableName}\\.[a-zA-Z0-9_]+)`, 'g');
  const columnMatch = sql.match(regex);
  return columnMatch;
};

/**
 * Replace the first SELECT * from the sqlToReplace with the cube measure
 * @param measures
 * @param tableSchema
 * @param sqlToReplace
 * @returns
 */
export const applyProjectionToSQLQuery = (
  dimensions: Member[],
  measures: Member[],
  tableSchema: TableSchema,
  sqlToReplace: string
) => {
  let measureSelectString = cubeMeasureToSQLSelectString(measures, tableSchema);

  if (measures.length > 0 && dimensions.length > 0) {
    measureSelectString += ', ';
  }
  const selectString = addDimensionToSQLProjection(
    dimensions,
    measureSelectString,
    tableSchema
  );

  return getSelectReplacedSql(sqlToReplace, selectString);
};
