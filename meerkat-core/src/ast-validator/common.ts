import { ParsedExpression } from '../types/duckdb-serialization-types';
import { ParsedSerialization } from './types';

export function validateSelectNode(
  parsedSerialization: ParsedSerialization
): ParsedExpression {
  const statement = parsedSerialization.statements?.[0];
  if (!statement) {
    throw new Error('No statement found');
  }

  if (statement.node.type !== 'SELECT_NODE') {
    throw new Error('Statement must be a SELECT node');
  }

  const selectList = statement.node.select_list;
  if (!selectList?.length || selectList.length !== 1) {
    throw new Error('SELECT must contain exactly one expression');
  }

  return selectList[0];
}

export const isError = (data: ParsedSerialization): boolean => {
  return !!data.error;
};
