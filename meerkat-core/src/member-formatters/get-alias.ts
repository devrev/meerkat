import { memberKeyToSafeKey } from './member-key-to-safe-key';

export const getAlias = (
  name: string,
  aliases?: Record<string, string>,
  safe?: boolean
): string => {
  return constructAlias(name, aliases?.[name], safe);
};

export const constructAlias = (
  name: string,
  alias?: string,
  safe?: boolean
): string => {
  if (alias) {
    if (safe) {
      // Alias may contain special characters or spaces, so we need to wrap in quotes.
      return `"${alias}"`;
    }
    return alias;
  }
  return memberKeyToSafeKey(name);
};
