import { COLUMN_NAME_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from './constants';

export const memberKeyToSafeKey = (
  memberKey: string,
  isDotDelimiterEnabled = false
) => {
  if (isDotDelimiterEnabled) {
    return memberKey; // Return as-is, caller handles quoting
  }
  return memberKey.split(COLUMN_NAME_DELIMITER).join(MEERKAT_OUTPUT_DELIMITER);
};
