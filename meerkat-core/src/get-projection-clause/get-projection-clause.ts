import { TableSchema } from "../types/cube-types";
import { findInSchema } from "../utils/find-in-table-schema";
import { memberKeyToSafeKey } from "../utils/member-key-to-safe-key";

export const getMemberProjection = ({ key, tableSchema }: {
  key: string;
  tableSchema: TableSchema;
}) => {
  // Find the table access key
  const measureWithoutTable = key.split('.')[1];

  const foundMember = findInSchema(measureWithoutTable, tableSchema)
  if (!foundMember) {
    // If the selected member is not found in the table schema or if it is already selected, continue.
    return {
      sql: undefined,
      foundMember: undefined,
      aliasKey: undefined
    }
  }
  const aliasKey = memberKeyToSafeKey(key);
  // Add the alias key to the set. So we have a reference to all the previously selected members.
  return { sql: `${foundMember.sql} AS ${aliasKey}` , foundMember, aliasKey }
}


export const getProjectionClause = (members: string[], tableSchema: TableSchema, aliasedColumnSet: Set<string>) => {
  return members.reduce((acc, member, currentIndex, members) => {
    const { sql: memberSql }  = getMemberProjection({ key: member, tableSchema })
    if (aliasedColumnSet.has(member) || !memberSql) {
      return acc
    }
    acc += `${memberSql}`
    if (currentIndex !== members.length - 1) {
      acc += `, `
    }
    return acc
  }, '')
}