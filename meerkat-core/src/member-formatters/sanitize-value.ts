/**
 * Sanitizes a string value for use in a SQL query
 */
export const sanitizeStringValue = (value: string) => {
  return value.replace(/'/g, "''");
};
