export const astSerializerQuery = (query: string) => {
  return `SELECT json_serialize_sql('${query}')`;
};
