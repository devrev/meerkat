import { Dimension, Measure, TableSchema } from '../types/cube-types/table';

export const getMemberInfoFromTableSchema = (
  memberKey: string,
  tableSchema: TableSchema,
) => {
  let memberInfo: Measure | Dimension | undefined;

  const memberKeyName = memberKey.split('.')[1];

  /**
   * Finding the table key from the measures.
   */
  for (let i = 0; i < tableSchema.measures.length; i++) {
    const measure = tableSchema.measures[i];
    const key = measure.name;
    if (!key || key !== memberKeyName) {
      continue;
    }

    memberInfo = measure;
    return memberInfo;
  }

  for (let i = 0; i < tableSchema.dimensions.length; i++) {
    const dimension = tableSchema.dimensions[i];
    const key = dimension.name;

    if (!key || key !== memberKeyName) {
      continue;
    }

    memberInfo = dimension;
    return memberInfo;
  }
  return;
};
