import { COLUMN_NAME_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from './constants';

export const memberKeyToSafeKey = (memberKey: string) => {
  return memberKey.split(COLUMN_NAME_DELIMITER).join(MEERKAT_OUTPUT_DELIMITER);
};
