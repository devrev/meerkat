import { LogicalType, PivotColumn, SampleOptions } from './Nodes';
import { ParsedExpression } from './ParsedExpression';
import { SelectStatement } from './Statement';

export enum TableReferenceType {
  INVALID = 'INVALID',
  BASE_TABLE = 'BASE_TABLE',
  SUBQUERY = 'SUBQUERY',
  JOIN = 'JOIN',
  TABLE_FUNCTION = 'TABLE_FUNCTION',
  EXPRESSION_LIST = 'EXPRESSION_LIST',
  CTE = 'CTE',
  EMPTY = 'EMPTY',
  PIVOT = 'PIVOT',
}

export interface BaseTableRef {
  type: TableReferenceType;
  alias: string;
  sample: SampleOptions | null;
}

export type TableRef =
  | BaseTypeTableRef
  | SubqueryRef
  | JoinRef
  | TableFunctionRef
  | ExpressionListRef
  | EmptyTableRef
  | PivotRef;

export interface BaseTypeTableRef extends BaseTableRef {
  type: TableReferenceType.BASE_TABLE;
  schema_name: string;
  table_name: string;
  column_name_alias: string[];
  catalog_name: string;
}

export enum JoinType {
  INVALID = 'INVALID',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
  INNER = 'INNER',
  OUTER = 'OUTER',
  SEMI = 'SEMI',
  ANTI = 'ANTI',
  MARK = 'MARK',
  SINGLE = 'SINGLE',
}

export enum JoinRefType {
  REGULAR = 'REGULAR',
  NATURAL = 'NATURAL',
  CROSS = 'CROSS',
  POSITIONAL = 'POSITIONAL',
  ASOF = 'ASOF',
  DEPENDENT = 'DEPENDENT',
}

export interface JoinRef extends BaseTableRef {
  type: TableReferenceType.JOIN;
  left: TableRef;
  right: TableRef;
  condition?: ParsedExpression;
  join_type: JoinType;
  ref_type: JoinRefType;
  using_columns: string[];
}

export interface SubqueryRef extends BaseTableRef {
  type: TableReferenceType.SUBQUERY;
  subquery: SelectStatement;
  column_name_alias: string[];
}

export interface TableFunctionRef extends BaseTableRef {
  type: TableReferenceType.TABLE_FUNCTION;
  function: ParsedExpression;
  column_name_alias: string[];
}

export interface EmptyTableRef extends BaseTableRef {
  type: TableReferenceType.EMPTY;
}

export interface ExpressionListRef extends BaseTableRef {
  type: TableReferenceType.EXPRESSION_LIST;
  expected_names: string[];
  expected_types: LogicalType[];
  values: ParsedExpression[][];
}

export interface PivotRef extends BaseTableRef {
  type: TableReferenceType.PIVOT;
  source: TableRef;
  aggregates: ParsedExpression[];
  unpivot_names: string[];
  pivots: PivotColumn[];
  groups: string[];
  column_name_alias: string[];
  include_nulls: boolean;
}
