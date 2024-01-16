import { MeerkatFilter, Member } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';


export const getAliasedColumnsFromFilters = ({ baseSql, members, meerkatFilters, tableSchema, tableSchemaAccessKey }: {
  members: Member[];
  meerkatFilters: MeerkatFilter[];
  tableSchema: TableSchema;
  baseSql: string;
  tableSchemaAccessKey: 'measures' | 'dimensions'
}) => {
  for (let i = 0; i < meerkatFilters.length; i++) {
    const { memberKey } = meerkatFilters[i];
    const measureWithoutTable = memberKey.split('.')[1];
    const aliasKey = memberKeyToSafeKey(memberKey);
    const measureSchema = tableSchema[tableSchemaAccessKey].find(
      (m) => m.name === measureWithoutTable
    );
    const isMeasureAlreadySelected = members.includes(memberKey);
    if (!measureSchema || isMeasureAlreadySelected) {
      continue;
    }
    if (i < meerkatFilters.length) {
      baseSql += ',';
    }
    baseSql += ` ${memberKey} AS ${aliasKey} `;
  }
  return baseSql
}


export const cubeMeasureToSQLSelectString = (
  measures: Member[],
  tableSchema: TableSchema,
  meerkatFilters: MeerkatFilter[]
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
      base += ', ';
    }
    base += ` ${measureSchema.sql} AS ${aliasKey} `;
  }
  return getAliasedColumnsFromFilters({ meerkatFilters, tableSchema, baseSql: base, members: measures, tableSchemaAccessKey: 'measures'});
};

const addDimensionToSQLProjection = (
  dimensions: Member[],
  selectString: string,
  tableSchema: TableSchema,
  meerkatFilters: MeerkatFilter[]
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
    newSelectString += `  ${dimensionSchema.sql} AS ${aliasKey}`;
  }
  return getAliasedColumnsFromFilters({ meerkatFilters, tableSchema, baseSql: newSelectString, members: dimensions, tableSchemaAccessKey: 'dimensions'});
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
  sqlToReplace: string,
  meerkatFilters: MeerkatFilter[]
) => {
  let measureSelectString = cubeMeasureToSQLSelectString(measures, tableSchema, meerkatFilters);

  if (measures.length > 0 && dimensions.length > 0) {
    measureSelectString += ', ';
  }
  const selectString = addDimensionToSQLProjection(
    dimensions,
    measureSelectString,
    tableSchema,
    meerkatFilters
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
