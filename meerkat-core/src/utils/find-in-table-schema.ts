import { splitIntoDataSourceAndFields } from '../member-formatters';
import { TableSchema } from '../types/cube-types';

export const findInDimensionSchema = (
  measure: string,
  tableSchema: TableSchema
) => {
  return tableSchema.dimensions.find((m) => m.name === measure);
};

export const findInMeasureSchema = (
  measure: string,
  tableSchema: TableSchema
) => {
  return tableSchema.measures.find((m) => m.name === measure);
};

export const findInSchema = (measure: string, tableSchema: TableSchema) => {
  /*
   ** Using the key passed as measureWithoutTable this function searches the table schema.
   ** It returns either the first dimension or measure found.
   */
  const foundDimension = findInDimensionSchema(measure, tableSchema);
  if (foundDimension) {
    return foundDimension;
  }
  const foundMeasure = findInMeasureSchema(measure, tableSchema);
  if (foundMeasure) {
    return foundMeasure;
  }
  return undefined;
};

export const findInDimensionSchemas = (
  name: string,
  tableSchemas: TableSchema[]
) => {
  /*
   ** Finds the dimension in the provided table schemas.
   ** Assumes the provided name is namespaced as `tableName.columnName`.
   */
  const [tableName, columnName] = splitIntoDataSourceAndFields(name);
  const tableSchema = tableSchemas.find((table) => table.name === tableName);
  if (!tableSchema) {
    return undefined;
  }
  return findInDimensionSchema(columnName, tableSchema);
};

export const findInSchemas = (name: string, tableSchemas: TableSchema[]) => {
  /*
   ** Finds the dimension or measure in the provided table schemas.
   ** Handles both namespaced (`tableName.columnName`) and non-namespaced names.
   */
  if (!name.includes('.')) {
    if (tableSchemas.length > 1) {
      throw new Error(
        `Multiple table schemas found for ${name} and field doesn't have a table name`
      );
    }
    return findInSchema(name, tableSchemas[0]);
  }

  const [tableName, columnName] = splitIntoDataSourceAndFields(name);
  const tableSchema = tableSchemas.find((table) => table.name === tableName);
  if (!tableSchema) {
    return undefined;
  }
  return findInSchema(columnName, tableSchema);
};
