export const getNamespacedKey = (namespace: string, name: string): string =>
  namespace ? `${namespace}.${name}` : name;
