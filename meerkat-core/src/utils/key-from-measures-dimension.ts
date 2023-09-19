import { Dimension, Measure, Query, TableSchema } from '@devrev/cube-types';

/**
 * Find the table key from the measures or dimensions.
 * The table key is the first part of the measure or dimension.
 * Example 1: 'orders.count' -> 'orders'
 * Example 2: 'orders.created_at' -> 'orders'
 * @param query
 * @returns
 */
export const tableKeyFromMeasuresDimension = (query: Query) => {
  let tableKey: string | null = null;

  /**
   * Finding the table key from the measures.
   */
  for (let i = 0; i < query.measures.length; i++) {
    const measure = query.measures[i];
    const key = measure.split('.')[0];

    if (!key) {
      continue;
    }

    tableKey = key;
    break;
  }

  /**
   * If a table key was found, return it.
   * We don't need to check the dimensions, assuming table can be just one in the query.
   */
  if (tableKey) {
    return tableKey;
  }

  if (!query?.dimensions?.length) {
    return null;
  }

  /**
   * Finding the table key from the dimensions.
   */
  for (let i = 0; i < query.dimensions.length; i++) {
    const dimension = query.dimensions[i];
    const key = dimension.split('.')[0];

    if (!key) {
      continue;
    }

    tableKey = key;
    break;
  }

  return tableKey;
};

export const getMemberInfoFromTableSchema = (
  memberKey: string,
  tableSchema: TableSchema
) => {
  let memberInfo: Measure | Dimension | undefined;

  /**
   * Finding the table key from the measures.
   */
  for (let i = 0; i < tableSchema.measures.length; i++) {
    const measure = tableSchema.measures[i];
    const key = measure.sql.split('.')[1];

    if (!key || key !== memberKey) {
      continue;
    }

    memberInfo = measure;
    return memberInfo;
  }

  for (let i = 0; i < tableSchema.dimensions.length; i++) {
    const dimension = tableSchema.dimensions[i];
    const key = dimension.sql.split('.')[1];

    if (!key || key !== memberKey) {
      continue;
    }

    memberInfo = dimension;
    return memberInfo;
  }

  return;
};
