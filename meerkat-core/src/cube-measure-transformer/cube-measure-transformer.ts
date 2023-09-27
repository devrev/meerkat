import { Member, TableSchema } from '@devrev/cube-types';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';

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
    const measureKeyWithoutTable = measure.split('.')[1];
    const aliasKey = memberKeyToSafeKey(measure);
    const measureSchema = tableSchema.measures.find(
      (m) => m.name === measureKeyWithoutTable
    );
    if (!measureSchema) {
      continue;
    }
    if (i > 0) {
      base += ',';
    }
    base += ` (${measureSchema.sql}) AS ${aliasKey} `;
  }
  return base;
};

const addDimensionToSQLProjection = (
  dimensions: Member[],
  selectString: string
) => {
  if (dimensions.length === 0) {
    return selectString;
  }
  let newSelectString = selectString;
  for (let i = 0; i < dimensions.length; i++) {
    const dimension = dimensions[i];
    if (i > 0) {
      newSelectString += ',';
    }
    newSelectString += ` ${dimension} AS ${dimension.replace('.', '__')}`;
  }
  return newSelectString;
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
    measureSelectString
  );

  const selectRegex = /SELECT\s\*/;
  const match = sqlToReplace.match(selectRegex);
  if (!match) {
    return sqlToReplace;
  }
  const selectIndex = match.index;
  if (selectIndex === undefined) {
    throw new Error('SELECT * not found in SQL string');
  }
  const selectLength = match[0].length;
  const beforeSelect = sqlToReplace.substring(0, selectIndex);
  const afterSelect = sqlToReplace.substring(selectIndex + selectLength);
  return `${beforeSelect}${selectString}${afterSelect}`;
};
