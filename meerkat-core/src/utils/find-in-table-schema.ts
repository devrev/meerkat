import { splitIntoDataSourceAndFields } from '../member-formatters';
import { Dimension, Measure, TableSchema } from '../types/cube-types';

/**
 * Picks the member whose `__sourceTable` matches `sourceTable` when more
 * than one entry shares the same `name`. Falls back to the first match
 * (legacy behavior) when no `__sourceTable` is set or no source-table
 * disambiguation is requested.
 *
 * Why: `getCombinedTableSchema` flat-maps members from every joined table
 * into one merged schema. When two tables expose members of the same
 * `name` (e.g. `count(id)` produces `id___function__count` on both sides
 * of a join), a naive `find(m => m.name === ...)` returns only the first
 * — so every cubeQuery key referencing that bare name resolves to one
 * table's SQL. Tagging members with their origin table during flat-map
 * and tie-breaking here keeps every cubeQuery key pointing at its own
 * table's member.
 */
const pickMember = <T extends Measure | Dimension>(
  members: T[],
  name: string,
  sourceTable: string | undefined
): T | undefined => {
  if (sourceTable) {
    const sourceMatch = members.find(
      (m) => m.name === name && m.__sourceTable === sourceTable
    );
    if (sourceMatch) return sourceMatch;
  }
  return members.find((m) => m.name === name);
};

export const findInDimensionSchema = (
  measure: string,
  tableSchema: TableSchema,
  sourceTable?: string
) => {
  return pickMember(tableSchema.dimensions, measure, sourceTable);
};

export const findInMeasureSchema = (
  measure: string,
  tableSchema: TableSchema,
  sourceTable?: string
) => {
  return pickMember(tableSchema.measures, measure, sourceTable);
};

export const findInSchema = (
  measure: string,
  tableSchema: TableSchema,
  sourceTable?: string
) => {
  /*
   ** Using the key passed as measureWithoutTable this function searches the table schema.
   ** It returns either the first dimension or measure found, preferring the
   ** member whose `__sourceTable` matches `sourceTable` when supplied — see
   ** `pickMember` above.
   */
  const foundDimension = findInDimensionSchema(
    measure,
    tableSchema,
    sourceTable
  );
  if (foundDimension) {
    return foundDimension;
  }
  const foundMeasure = findInMeasureSchema(measure, tableSchema, sourceTable);
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
  // TODO: Move to only using namespaced keys.
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
