export const addNamespace = (namespace: string, name: string): string =>
  namespace ? `${namespace}.${name}` : name;
