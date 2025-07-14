import { TableSchema } from '../types/cube-types';

export const getAliases = (
  tableSchemas: TableSchema[]
): Record<string, string> => {
  const aliases: Record<string, string> = {};

  tableSchemas.forEach((table) => {
    table.measures.forEach((measure) => {
      if (measure.alias) {
        aliases[`${table.name}.${measure.name}`] = measure.alias;
      }
    });
    table.dimensions.forEach((dimension) => {
      if (dimension.alias) {
        aliases[`${table.name}.${dimension.name}`] = dimension.alias;
      }
    });
  });
  return aliases;
};
