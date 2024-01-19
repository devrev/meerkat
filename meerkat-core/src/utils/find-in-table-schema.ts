import { TableSchema } from "../types/cube-types"

export const findInSchema = (measureWithoutTable: string, tableSchema: TableSchema) => {
    /*
    ** Using the key passed as measureWithoutTable this function searches the table schema.
    ** It returns either the first dimension or measure found.
    */
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
  