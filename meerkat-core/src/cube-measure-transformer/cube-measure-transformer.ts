import { GenericFilter, Member } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';


export const getAliasedColumnsFromFilters = ({ baseSql, members, meerkatFilters, tableSchema }: {
  members: Member[];
  meerkatFilters: GenericFilter[];
  tableSchema: TableSchema;
  baseSql: string;
}) => {
  /*
   * This function returns the string of aliased columns from the filters passed.
   */
  const aliasedColumnsSet = new Set<string>();
  /*  
   * Maintain a set to make sure already seen members are not added again. 
   */
  let sql = baseSql
  if (!meerkatFilters) {
    return baseSql;
  }
  for (let i = 0; i < meerkatFilters.length; i++) {
    const filter = meerkatFilters[i]
    if ('and' in filter) {
    // Traverse through the passed 'and' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        members,
        meerkatFilters: filter.and,
        tableSchema,
      })
    }
    if ('or' in filter) {
    // Traverse through the passed 'or' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        tableSchema,
        members,
        meerkatFilters: filter.or,
      })
    } 
    if ('member' in filter) {
      const { member } = filter;
      // Find the table access key
      const tableColumn = member.split('__').join('.')
      const measureWithoutTable = member.split('__')[1];
      const aliasKey = memberKeyToSafeKey(member);

      const foundMember = findInSchema(measureWithoutTable, tableSchema)
      if (!foundMember  || aliasedColumnsSet.has(aliasKey)) {
        // If the selected member is not found in the table schema or if it is already selected, continue.
        continue;
      }
      // Add the alias key to the set. So we have a reference to all the previously selected members.
      aliasedColumnsSet.add(aliasKey)
      sql += `, ${tableColumn} AS ${aliasKey} `;
    }
  }
  return sql
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

  return getSelectReplacedSql(sqlToReplace, selectString)
};
