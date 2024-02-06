import { MEERKAT_INPUT_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from "./constants";

export const memberKeyToSafeKey = (memberKey: string) => {
  return memberKey.split(MEERKAT_INPUT_DELIMITER).join(MEERKAT_OUTPUT_DELIMITER);
};
