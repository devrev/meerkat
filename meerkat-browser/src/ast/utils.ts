import { ParsedSerialization } from '@devrev/meerkat-core';

export const isParseError = (data: ParsedSerialization): boolean => {
  return !!data.error;
};
