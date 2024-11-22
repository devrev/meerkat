import { ParsedSerialization } from './types';

export const isError = (data: ParsedSerialization): boolean => {
  return !!data.error;
};
