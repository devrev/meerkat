import { astSerializerQuery } from '../ast-serializer/ast-serializer';
import { deserializeQuery } from '../ast-deserializer/ast-deserializer';
import { sanitizeStringValue } from '../member-formatters/sanitize-value';
import { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';

/**
 * Replaces redundant `SELECT <cols>, * FROM (<join>)` star projections with an
 * explicit, collision-free column list (ISS-301213).
 *
 * ## The bug
 *
 * The resolution pipeline wraps each layer as
 * `SELECT <base columns, explicitly aliased>, * FROM (<inner>)`. When `<inner>`
 * is a multi-table JOIN (the base query left-joined against several lookup
 * tables) the trailing `*` re-exposes the raw columns of *every* joined table.
 * If two lookups share a column name (e.g. `devrev_copy` across `dim_group` +
 * `dim_devu`), that name becomes ambiguous and DuckDB rejects the export with
 * `Binder Error: Ambiguous reference to column name ...`.
 *
 * ## Why we cannot simply delete the star
 *
 * The star is load-bearing: the resolved lookup columns (e.g.
 * `..__owner_id__fullname`) are produced inside the joined subqueries and reach
 * the outer query *only* through it. Deleting it drops those columns
 * ("Referenced column ... not found"). The star must be REPLACED by the exact
 * columns the rest of the statement still references.
 *
 * ## Surgical, formatting-preserving rewrite
 *
 * We parse the SQL into DuckDB's AST only to *analyse* it, then edit the
 * ORIGINAL SQL string in place using the `query_location` byte offset DuckDB
 * attaches to every star node. Each offending `*` character is replaced with an
 * explicit column list; every other byte of the SQL is left untouched. This
 * avoids re-serializing the whole statement (which would cosmetically rewrite
 * keyword case, quoting, parenthesisation, etc.).
 *
 * ## Algorithm
 *
 * 1. Collect every column referenced anywhere in the statement (`globalRefs`).
 * 2. Find each bare star whose enclosing `SELECT` reads from a JOIN.
 * 3. Compute the column names that JOIN exposes (from its child subqueries).
 * 4. Replace the star with the exposed columns that are referenced somewhere
 *    and not already projected by a sibling. Columns nobody references — the
 *    ambiguous raw passthrough like `devrev_copy` — are left out, removing the
 *    collision while preserving every needed column.
 *
 * Bare star = a `STAR` with no `EXCLUDE` / `REPLACE` / `relation_name`. Those
 * are intentional and are never touched, and a lone `SELECT *` is left as-is.
 */

const STAR_CLASS = 'STAR';
const COLUMN_REF_CLASS = 'COLUMN_REF';
const SELECT_NODE_TYPE = 'SELECT_NODE';
const JOIN_TABLE_TYPE = 'JOIN';
const SUBQUERY_TABLE_TYPE = 'SUBQUERY';

interface AstNode {
  class?: string;
  type?: string;
  alias?: string;
  relation_name?: string;
  exclude_list?: unknown[];
  replace_list?: unknown[];
  columns?: boolean;
  expr?: unknown;
  column_names?: string[];
  query_location?: number;
  select_list?: AstNode[];
  from_table?: AstNode;
  left?: AstNode;
  right?: AstNode;
  subquery?: { node?: AstNode };
  [key: string]: unknown;
}

const isBareStar = (item: AstNode): boolean => {
  return (
    item?.class === STAR_CLASS &&
    !item.relation_name &&
    (!item.exclude_list || item.exclude_list.length === 0) &&
    (!item.replace_list || item.replace_list.length === 0) &&
    !item.columns &&
    !item.expr
  );
};

/** Last segment of a `COLUMN_REF`'s `column_names` (the bare column name). */
const refName = (item: AstNode): string | undefined => {
  if (item?.class !== COLUMN_REF_CLASS || !Array.isArray(item.column_names)) {
    return undefined;
  }
  return item.column_names[item.column_names.length - 1];
};

/** Every bare column name referenced by a `COLUMN_REF` anywhere under `root`. */
const collectGlobalColumnRefs = (root: unknown): Set<string> => {
  const refs = new Set<string>();
  const stack: unknown[] = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') continue;
    if (Array.isArray(current)) {
      current.forEach((item) => stack.push(item));
      continue;
    }
    const record = current as AstNode;
    const name = refName(record);
    if (name) refs.add(name);
    Object.values(record).forEach((value) => stack.push(value));
  }
  return refs;
};

/**
 * The set of column names a FROM table exposes upward that we can ENUMERATE.
 *
 * - A JOIN exposes the union of both sides' enumerable columns.
 * - A SUBQUERY exposes the explicit aliases/column-names in its select list. If
 *   that select list itself contains a star, the star contributes an unknown
 *   set of raw columns — never referenced by the resolution pipeline — so we
 *   skip it rather than bailing out.
 * - Base tables (and anything else) expose a non-enumerable set → `null`, which
 *   tells the caller to leave the star untouched.
 */
const getExposedColumns = (table: AstNode | null | undefined): string[] | null => {
  if (!table) return null;

  if (table.type === JOIN_TABLE_TYPE) {
    const left = getExposedColumns(table.left);
    const right = getExposedColumns(table.right);
    if (left === null || right === null) return null;
    return [...left, ...right];
  }

  if (table.type === SUBQUERY_TABLE_TYPE) {
    const node = table.subquery?.node;
    if (!node || !Array.isArray(node.select_list)) return null;
    const names: string[] = [];
    for (const item of node.select_list) {
      if (item?.class === STAR_CLASS) {
        // Inner star exposes raw passthrough columns we cannot enumerate; they
        // are never referenced upward in the resolution pipeline, so omitting
        // them is safe — skip rather than abort.
        continue;
      }
      const columnName = item?.alias || refName(item);
      if (!columnName) return null;
      names.push(columnName);
    }
    return names;
  }

  return null;
};

interface StarEdit {
  location: number;
  replacement: string;
}

const quoteIdentifier = (name: string): string => `"${name.replace(/"/g, '""')}"`;

/**
 * Walks the AST collecting a string edit for each bare-star-over-JOIN node: the
 * byte offset of its `*` and the explicit column list that should replace it.
 * Stars whose needed-column set is empty (everything already projected) or
 * whose JOIN columns cannot be enumerated are skipped.
 */
const collectStarEdits = (root: unknown, globalRefs: Set<string>): StarEdit[] => {
  const edits: StarEdit[] = [];
  const stack: unknown[] = [root];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') continue;
    if (Array.isArray(current)) {
      current.forEach((item) => stack.push(item));
      continue;
    }
    const record = current as AstNode;

    if (
      record.type === SELECT_NODE_TYPE &&
      Array.isArray(record.select_list) &&
      record.from_table?.type === JOIN_TABLE_TYPE
    ) {
      const star = record.select_list.find(
        (item: AstNode) => isBareStar(item) && typeof item.query_location === 'number'
      );

      if (star) {
        const exposed = getExposedColumns(record.from_table);
        if (exposed !== null) {
          const alreadyProjected = new Set<string>(
            record.select_list
              .map(refName)
              .filter((n: string | undefined): n is string => Boolean(n))
          );

          const seen = new Set<string>();
          const needed: string[] = [];
          for (const name of exposed) {
            if (
              globalRefs.has(name) &&
              !alreadyProjected.has(name) &&
              !seen.has(name)
            ) {
              seen.add(name);
              needed.push(name);
            }
          }

          // Replace `*` with the explicit needed columns. If none are needed,
          // drop the `*` entirely by collapsing the preceding ", " — handled by
          // the caller via an empty replacement marker.
          if (typeof star.query_location === 'number') {
            edits.push({
              location: star.query_location,
              replacement: needed.map(quoteIdentifier).join(', '),
            });
          }
        }
      }
    }

    Object.values(record).forEach((value) => stack.push(value));
  }

  return edits;
};

/**
 * Converts a DuckDB `query_location` (a UTF-8 BYTE offset) into a JS string
 * (UTF-16 code-unit) index. They diverge whenever the SQL contains a multi-byte
 * character (e.g. an accented name or emoji in a literal) before the offset, so
 * indexing the JS string directly with the raw byte offset would point at the
 * wrong character.
 */
const byteOffsetToCharIndex = (sql: string, byteOffset: number): number => {
  return Buffer.from(sql, 'utf8').subarray(0, byteOffset).toString('utf8').length;
};

/**
 * Applies star edits to the original SQL string. Each edit replaces the single
 * `*` character (located via its byte offset, converted to a char index) with
 * the explicit column list. When the replacement is empty (no columns needed),
 * the comma separating the star from its neighbour is removed too — the
 * preceding `, ` if present, otherwise a following `, ` (covers a leading star
 * such as `SELECT *, x`) — so we never leave a dangling comma. Edits are
 * applied right-to-left so earlier offsets stay valid.
 */
const applyStarEdits = (sql: string, edits: StarEdit[]): string => {
  const ordered = [...edits].sort((a, b) => b.location - a.location);
  let result = sql;

  for (const edit of ordered) {
    const at = byteOffsetToCharIndex(result, edit.location);
    if (result[at] !== '*') {
      // Offset no longer points at a star (defensive); skip this edit.
      continue;
    }

    if (edit.replacement.length > 0) {
      result = result.slice(0, at) + edit.replacement + result.slice(at + 1);
      continue;
    }

    // Empty replacement: drop the `*` and the comma that separated it from a
    // sibling projection. Prefer the preceding `, `; if the star was first in
    // the list (`SELECT *, x`), strip the following `, ` instead.
    const before = result.slice(0, at);
    const after = result.slice(at + 1);
    const precedingComma = before.match(/,\s*$/);
    if (precedingComma) {
      result = result.slice(0, at - precedingComma[0].length) + after;
      continue;
    }
    const followingComma = after.match(/^\s*,\s*/);
    if (followingComma) {
      result = before + after.slice(followingComma[0].length);
      continue;
    }
    // No separating comma (lone star) — just remove the `*`.
    result = before + after;
  }

  return result;
};

export interface PruneRedundantStarsResult {
  sql: string;
  rewrittenStars: number;
}

/**
 * Parses `sql` into a DuckDB AST, then surgically rewrites each redundant
 * post-join star projection into an explicit collision-free column list,
 * editing the original SQL string in place (formatting preserved). On any
 * failure — DuckDB rejecting the serialize query, a malformed serialization,
 * or a parse error — the original SQL is returned unchanged so the export path
 * degrades gracefully (it never throws).
 *
 * @param sql - the fully-assembled resolution/export SQL
 * @param executeQuery - runs a query against DuckDB and returns its rows
 */
export const pruneRedundantStarsOverJoins = async (
  sql: string,
  executeQuery: GetQueryOutput
): Promise<PruneRedundantStarsResult> => {
  let serialized: { error?: boolean; statements?: AstNode[] };
  try {
    const serializedRows = await executeQuery(
      astSerializerQuery(sanitizeStringValue(sql))
    );
    serialized = JSON.parse(deserializeQuery(serializedRows));
  } catch {
    // DuckDB rejected the serialize query or the payload could not be parsed —
    // leave the SQL untouched rather than breaking the export.
    return { sql, rewrittenStars: 0 };
  }

  if (serialized.error || !Array.isArray(serialized.statements)) {
    return { sql, rewrittenStars: 0 };
  }

  const globalRefs = collectGlobalColumnRefs(serialized.statements);
  const edits = collectStarEdits(serialized.statements, globalRefs);
  if (edits.length === 0) {
    return { sql, rewrittenStars: 0 };
  }

  const rewrittenSql = applyStarEdits(sql, edits);
  return { sql: rewrittenSql, rewrittenStars: edits.length };
};
