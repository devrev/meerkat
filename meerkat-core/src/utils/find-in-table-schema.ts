import { TableSchema } from "../types/cube-types"


export const findInDimensionSchema = (measure: string, tableSchema: TableSchema) => {
  return tableSchema.dimensions.find(
    (m) => m.name === measure
  )
}

export const findInMeasureSchema = (measure: string, tableSchema: TableSchema) => {
  return tableSchema.measures.find(
    (m) => m.name === measure
  )
}

export const findInSchema = (measure: string, tableSchema: TableSchema) => {
  /*
  ** Using the key passed as measureWithoutTable this function searches the table schema.
  ** It returns either the first dimension or measure found.
  */
  const foundDimension = findInDimensionSchema(measure, tableSchema)
  if (foundDimension) {
    return foundDimension
  }
  const foundMeasure = findInMeasureSchema(measure, tableSchema)
  if (foundMeasure) {
    return foundMeasure
  }
  return undefined
}
