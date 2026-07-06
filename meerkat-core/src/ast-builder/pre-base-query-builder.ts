import { getAliasForAST } from '../member-formatters/get-alias';
import { Query, TableSchema } from '../types/cube-types';
import { MeerkatQueryFilter } from '../types/cube-types/index';
import { BASE_TABLE_NAME } from '../utils/base-ast';

/**
 * POC: AST-free, synchronous builder for `preBaseQuery`.
 *
 * The `cubeToDuckdbAST -> json_deserialize_sql` round-trip in the generation
 * pipeline only produces the outer query skeleton:
 *
 *   SELECT * FROM REPLACE_BASE_TABLE [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT ..]
 *
 * The SELECT list and FROM target are placeholders that downstream string
 * replacements overwrite anyway, so the only real output is the WHERE / GROUP
 * BY / ORDER BY / LIMIT clauses.
 *
 * This builder reproduces that string directly for the shapes where the WHERE
 * clause is empty (no projection filters). The WHERE/HAVING operator grammar is
 * intentionally out of scope for the POC — `canBuildPreBaseQuerySync` returns
 * false for those, so the caller falls back to the AST round-trip.
 */

/**
 * Reproduce DuckDB's identifier rendering for a COLUMN_REF the way
 * `json_deserialize_sql` emits it: a bare identifier is left unquoted, anything
 * that is not a simple `[A-Za-z_][A-Za-z0-9_]*` token is wrapped in double
 * quotes (with embedded quotes doubled). `getAliasForAST` hands us the raw
 * alias (unquoted) because the AST path relies on DuckDB to quote on output;
 * here we must do that quoting ourselves to stay byte-identical.
 */
const SIMPLE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

const renderIdentifier = (identifier: string): string => {
  if (SIMPLE_IDENTIFIER.test(identifier)) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
};

const isEmptyFilterGroup = (filter: MeerkatQueryFilter): boolean => {
  if ('member' in filter) {
    return false;
  }
  if ('and' in filter) {
    return filter.and.every(isEmptyFilterGroup);
  }
  if ('or' in filter) {
    return filter.or.every(isEmptyFilterGroup);
  }
  // Unknown shape — treat as non-empty to be safe (forces AST fallback).
  return false;
};

/**
 * True when the AST-free builder can safely produce a byte-identical
 * `preBaseQuery`. Currently limited to queries whose filters contribute no
 * WHERE/HAVING clause (empty filter groups or none at all).
 */
export const canBuildPreBaseQuerySync = (query: Query): boolean => {
  const filters = query.filters;
  if (!filters || filters.length === 0) {
    return true;
  }
  return filters.every(isEmptyFilterGroup);
};

/**
 * Build the `preBaseQuery` string without constructing an AST or hitting
 * DuckDB. Callers MUST gate on `canBuildPreBaseQuerySync(query)` first.
 */
export const buildPreBaseQuerySync = (
  query: Query,
  tableSchema: TableSchema
): string => {
  const clauses: string[] = [`SELECT * FROM ${BASE_TABLE_NAME}`];

  // GROUP BY: only when the query aggregates (measures present) and projects
  // dimensions — matches `cubeToDuckdbAST`'s guard.
  const dimensions = query.dimensions ?? [];
  if (query.measures.length > 0 && dimensions.length > 0) {
    const groupByCols = dimensions.map((dimension) =>
      renderIdentifier(getAliasForAST(dimension, tableSchema))
    );
    clauses.push(`GROUP BY ${groupByCols.join(', ')}`);
  }

  // ORDER BY: one entry per order key, alias + direction.
  if (query.order && Object.keys(query.order).length > 0) {
    const orderParts = Object.entries(query.order).map(([key, direction]) => {
      const alias = renderIdentifier(getAliasForAST(key, tableSchema));
      const dir = direction === 'desc' ? 'DESC' : 'ASC';
      return `${alias} ${dir}`;
    });
    clauses.push(`ORDER BY ${orderParts.join(', ')}`);
  }

  // LIMIT / OFFSET.
  if (query.limit || query.offset) {
    let limitClause = '';
    if (query.limit) {
      limitClause += `LIMIT ${query.limit}`;
    }
    if (query.offset) {
      limitClause += limitClause ? ` OFFSET ${query.offset}` : `OFFSET ${query.offset}`;
    }
    clauses.push(limitClause);
  }

  return clauses.join(' ');
};
