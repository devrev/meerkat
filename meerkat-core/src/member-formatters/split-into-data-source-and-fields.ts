import { COLUMN_NAME_DELIMITER } from './constants';

export const splitIntoDataSourceAndFields = (member: string) => {
  const [dataSource, ...fields] = member.split(COLUMN_NAME_DELIMITER);
  return [dataSource, fields.join(COLUMN_NAME_DELIMITER)];
};
