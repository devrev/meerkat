import { memberKeyToSafeKey } from './member-key-to-safe-key';

export const getAlias = (name: string, alias?: string): string => {
  return alias ? `"${alias}"` : memberKeyToSafeKey(name);
};
