import { GenericFilter, Member } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';

const findInSchema = (measureWithoutTable: string, tableSchema: TableSchema) => {
  const foundDimension = tableSchema.dimensions.find(
    (m) => m.name === measureWithoutTable
  )
  if (foundDimension) {
    return foundDimension
  }
  const foundMeasure = tableSchema.measures.find(
    (m) => m.name === measureWithoutTable
  )
  if (foundMeasure) {
    return foundMeasure
  }
  return undefined
}


export const getAliasedColumnsFromFilters = ({ baseSql, members, meerkatFilters, tableSchema }: {
  members: Member[];
  meerkatFilters: GenericFilter[];
  tableSchema: TableSchema;
  baseSql: string;
}) => {
  const aliasedColumnsSet = new Set<string>();
  if (!meerkatFilters) {
    return baseSql;
  }
  for (let i = 0; i < meerkatFilters.length; i++) {
    const filter = meerkatFilters[i]
    if ('and' in filter) {
      baseSql += getAliasedColumnsFromFilters({
        baseSql: '',
        members,
        meerkatFilters: filter.and,
        tableSchema,
      })
    } else if ('or' in filter) {
      baseSql += getAliasedColumnsFromFilters({
        baseSql: '',
        tableSchema,
        members,
        meerkatFilters: filter.or,
      })
    } else {
      const { member } = filter;
      const tableColumn = member.split('__').join('.')
      const measureWithoutTable = member.split('__')[1];
      const aliasKey = memberKeyToSafeKey(member);
      const foundMember = findInSchema(measureWithoutTable, tableSchema)
      
      const isMeasureAlreadySelected = members.includes(member);
      if (!foundMember || isMeasureAlreadySelected || aliasedColumnsSet.has(aliasKey)) {
        continue;
      }
      aliasedColumnsSet.add(aliasKey)
      baseSql += `, ${tableColumn} AS ${aliasKey} `;
    }
  }
  return baseSql
}


export const cubeMeasureToSQLSelectString = (
  measures: Member[],
  tableSchema: TableSchema,
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
  return base
};

const addDimensionToSQLProjection = (
  dimensions: Member[],
  selectString: string,
  tableSchema: TableSchema,
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
  return newSelectString
};

export const getReplacedSQL = (sql: string, selectString: string) => {
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
}

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
) => {
  let measureSelectString = cubeMeasureToSQLSelectString(measures, tableSchema);

  if (measures.length > 0 && dimensions.length > 0) {
    measureSelectString += ', ';
  }
  const selectString = addDimensionToSQLProjection(
    dimensions,
    measureSelectString,
    tableSchema,
  );

  return getReplacedSQL(sqlToReplace, selectString)
};
