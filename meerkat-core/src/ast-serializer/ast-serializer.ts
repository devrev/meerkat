/** Escapes single quotes for embedding SQL inside json_serialize_sql('...'). */
export function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

export const astSerializerQuery = (query: string) => {
  return `SELECT json_serialize_sql('${query}')`;
};
