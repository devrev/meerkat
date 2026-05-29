import {
  SelectNode,
  TableRef,
  TableReferenceType,
} from '../types/duckdb-serialization-types';

/**
 * Extracts the primary table name from a SelectNode's FROM clause.
 *
 * Resolution rules:
 * - BASE_TABLE: alias (if present) or table_name
 * - SUBQUERY: alias or fallback "subquery"
 * - JOIN: recurse into left side (the primary/driving table)
 * - TABLE_FUNCTION or other: alias or fallback "query"
 * - No FROM clause: "query"
 */
export function extractTableName(selectNode: SelectNode): string {
  if (!selectNode.from_table) return 'query';
  return resolveTableRef(selectNode.from_table);
}

// Resolves the "primary" table name from a FROM clause reference.
// This name becomes the namespace prefix for all members (e.g. "t.status", "tickets.cnt").
function resolveTableRef(ref: TableRef): string {
  // FROM tickets AS t → use alias "t"; FROM tickets → use table_name "tickets"
  if (ref.type === TableReferenceType.BASE_TABLE) {
    return ref.alias || ref.table_name || 'query';
  }
  // FROM (SELECT ...) AS sub → use alias "sub"; unnamed subquery → "subquery"
  if (ref.type === TableReferenceType.SUBQUERY) {
    return ref.alias || 'subquery';
  }
  // FROM tickets t JOIN users u ON ... → recurse into LEFT (driving) table
  // JOINs are left-associative: A JOIN B JOIN C → left=A-JOIN-B, right=C
  if (ref.type === TableReferenceType.JOIN) {
    return resolveTableRef(ref.left);
  }
  // TABLE_FUNCTION (generate_series), EMPTY (no FROM), PIVOT, etc.
  // Use alias if provided, otherwise generic "query" fallback
  return ref.alias || 'query';
}
