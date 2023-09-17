import { Constraint } from './Constraint';
import { MacroFunction } from './MacroFunction';
import { Value } from './Misc';
import { ColumnList, LogicalType } from './Nodes';
import { ParsedExpression } from './ParsedExpression';
import { SelectStatement } from './Statement';

export enum OnCreateConflict {
  ERROR_ON_CONFLICT = 'ERROR_ON_CONFLICT',
  IGNORE_ON_CONFLICT = 'IGNORE_ON_CONFLICT',
  REPLACE_ON_CONFLICT = 'REPLACE_ON_CONFLICT',
  ALTER_ON_CONFLICT = 'ALTER_ON_CONFLICT',
}

export enum CatalogType {
  INVALID = 'INVALID',
  TABLE_ENTRY = 'TABLE_ENTRY',
  SCHEMA_ENTRY = 'SCHEMA_ENTRY',
  VIEW_ENTRY = 'VIEW_ENTRY',
  INDEX_ENTRY = 'INDEX_ENTRY',
  PREPARED_STATEMENT = 'PREPARED_STATEMENT',
  SEQUENCE_ENTRY = 'SEQUENCE_ENTRY',
  COLLATION_ENTRY = 'COLLATION_ENTRY',
  TYPE_ENTRY = 'TYPE_ENTRY',
  DATABASE_ENTRY = 'DATABASE_ENTRY',

  // functions
  TABLE_FUNCTION_ENTRY = 'TABLE_FUNCTION_ENTRY',
  SCALAR_FUNCTION_ENTRY = 'SCALAR_FUNCTION_ENTRY',
  AGGREGATE_FUNCTION_ENTRY = 'AGGREGATE_FUNCTION_ENTRY',
  PRAGMA_FUNCTION_ENTRY = 'PRAGMA_FUNCTION_ENTRY',
  COPY_FUNCTION_ENTRY = 'COPY_FUNCTION_ENTRY',
  MACRO_ENTRY = 'MACRO_ENTRY',
  TABLE_MACRO_ENTRY = 'TABLE_MACRO_ENTRY',

  // version info
  UPDATED_ENTRY = 'UPDATED_ENTRY',
  DELETED_ENTRY = 'DELETED_ENTRY',
}

export interface BaseCreateInfo {
  type: CatalogType;
  catalog: string;
  schema: string;
  temporary: boolean;
  internal: boolean;
  on_conflict: OnCreateConflict;
  sql: string;
}

export type CreateInfo =
  | CreateIndexInfo
  | CreateTableInfo
  | CreateSchemaInfo
  | CreateViewInfo
  | CreateTypeInfo
  | CreateMacroInfo
  | CreateSequenceInfo;

export enum IndexType {
  INVALID = 'INVALID',
  ART = 'ART',
  EXTENSION = 'EXTENSION',
}

export enum IndexConstraintType {
  NONE = 'NONE',
  UNIQUE = 'UNIQUE',
  PRIMARY = 'PRIMARY',
  FOREIGN = 'FOREIGN',
}

export interface CreateIndexInfo extends BaseCreateInfo {
  name: string;
  table: string;
  index_type: IndexType;
  constraint_type: IndexConstraintType;
  parsed_expressions: ParsedExpression[];
  names: string[];
  column_ids: number[];
  options: Record<string, Value>;
  index_type_name: string;
}

export interface CreateTableInfo extends BaseCreateInfo {
  table: string;
  columns: ColumnList;
  constraints: Constraint[];
  query?: SelectStatement;
}

export interface CreateSchemaInfo extends BaseCreateInfo {
  // No additional members
}

export interface CreateViewInfo extends BaseCreateInfo {
  view_name: string;
  aliases: string[];
  types: LogicalType[];
  query?: SelectStatement;
}

export interface CreateTypeInfo extends BaseCreateInfo {
  name: string;
  logical_type: LogicalType;
}

export interface CreateMacroInfo extends BaseCreateInfo {
  name: string;
  function: MacroFunction;
}

export interface CreateSequenceInfo extends BaseCreateInfo {
  name: string;
  usage_count: number;
  increment: number;
  min_value: number;
  max_value: number;
  start_value: number;
  cycle: boolean;
}
