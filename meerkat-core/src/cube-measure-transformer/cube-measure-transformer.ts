import { Member } from '../types/cube-types/query';
import { Measure, TableSchema } from '../types/cube-types/table';
import { meerkatPlaceholderReplacer } from '../utils/meerkat-placeholder-replacer';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';

export const cubeMeasureToSQLSelectString = (
  measures: Member[],
  joinedTableSchema: TableSchema
) => {
  let base = 'SELECT';
  for (let i = 0; i < measures.length; i++) {
    const measure = measures[i];
    if (measure === '*') {
      base += ` ${joinedTableSchema.name}.*`;
      continue;
    }
    const tableSchemaName = measure.split('.')[0];
    const measureKeyWithoutTable = measure.split('.')[1];
    const aliasKey = memberKeyToSafeKey(measure);
    const measureSchema = joinedTableSchema.measures.find(
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
      joinedTableSchema.name
    );
    const columnsUsedInMeasure = applyRegexToSQL(
      meerkatReplacedSqlString,
      tableSchemaName
    );

    //Replace all the columnsUsedInMeasure with safeKey
    columnsUsedInMeasure?.forEach((measureKey) => {
      const column = measureKey.split('.')[1];
      console.info('column', column, tableSchemaName);
      const columnKey = memberKeyToSafeKey(`${tableSchemaName}.${column}`);
      meerkatReplacedSqlString = meerkatReplacedSqlString.replace(
        `${tableSchemaName}.${column}`,
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
  tableSchema: TableSchema
) => {
  if (dimensions.length === 0) {
    return selectString;
  }
  let newSelectString = selectString;
  for (let i = 0; i < dimensions.length; i++) {
    const dimension = dimensions[i];
    const dimensionKeyWithoutTable = dimension.split('.')[1];
    const dimensionSchema = tableSchema.dimensions.find(
      (m) => m.name === dimensionKeyWithoutTable
    );
    const aliasKey = memberKeyToSafeKey(dimension);

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

export const getAllColumnUsedInMeasures = (
  measures: Measure[],
  tableSchema: TableSchema
) => {
  let columns: string[] = [];
  measures.forEach((measure) => {
    const columnMatch = applyRegexToSQL(measure.sql, tableSchema.name);
    if (columnMatch && columnMatch.length > 0) {
      columns = [...columns, ...columnMatch];
    }
  });
  // Remove duplicates
  return [...new Set(columns)];
};

const applyRegexToSQL = (sql: string, tableName: string) => {
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
