import { PhysicalIndex } from './Constraint';
import { Value } from './Misc';
import { ColumnDefinition, LogicalType } from './Nodes';
import { ParsedExpression } from './ParsedExpression';

export enum ParseInfoType {
  ALTER_INFO = 'ALTER_INFO',
  ATTACH_INFO = 'ATTACH_INFO',
  COPY_INFO = 'COPY_INFO',
  CREATE_INFO = 'CREATE_INFO',
  DETACH_INFO = 'DETACH_INFO',
  DROP_INFO = 'DROP_INFO',
  BOUND_EXPORT_DATA = 'BOUND_EXPORT_DATA',
  LOAD_INFO = 'LOAD_INFO',
  PRAGMA_INFO = 'PRAGMA_INFO',
  SHOW_SELECT_INFO = 'SHOW_SELECT_INFO',
  TRANSACTION_INFO = 'TRANSACTION_INFO',
  VACUUM_INFO = 'VACUUM_INFO',
}

export interface ParseInfo {
  info_type: ParseInfoType;
}

export enum AlterType {
  INVALID = 'INVALID',
  ALTER_TABLE = 'ALTER_TABLE',
  ALTER_VIEW = 'ALTER_VIEW',
  ALTER_SEQUENCE = 'ALTER_SEQUENCE',
  CHANGE_OWNERSHIP = 'CHANGE_OWNERSHIP',
  ALTER_SCALAR_FUNCTION = 'ALTER_SCALAR_FUNCTION',
  ALTER_TABLE_FUNCTION = 'ALTER_TABLE_FUNCTION',
}

export enum OnEntryNotFound {
  THROW_EXCEPTION = 'THROW_EXCEPTION',
  RETURN_NULL = 'RETURN_NULL',
}

export interface AlterInfo extends ParseInfo {
  type: AlterType;
  catalog: string;
  schema: string;
  name: string;
  if_not_found: OnEntryNotFound;
  allow_internal: boolean;
}

export enum AlterTableType {
  INVALID = 'INVALID',
  RENAME_COLUMN = 'RENAME_COLUMN',
  RENAME_TABLE = 'RENAME_TABLE',
  ADD_COLUMN = 'ADD_COLUMN',
  REMOVE_COLUMN = 'REMOVE_COLUMN',
  ALTER_COLUMN_TYPE = 'ALTER_COLUMN_TYPE',
  SET_DEFAULT = 'SET_DEFAULT',
  FOREIGN_KEY_CONSTRAINT = 'FOREIGN_KEY_CONSTRAINT',
  SET_NOT_NULL = 'SET_NOT_NULL',
  DROP_NOT_NULL = 'DROP_NOT_NULL',
}

export interface AlterTableInfo extends AlterInfo {
  alter_table_type: AlterTableType;
}

export interface RenameColumnInfo extends AlterTableInfo {
  old_name: string;
  new_name: string;
}

export interface RenameTableInfo extends AlterTableInfo {
  new_table_name: string;
}

export interface AddColumnInfo extends AlterTableInfo {
  new_column: ColumnDefinition;
  if_column_not_exists: boolean;
}

export interface RemoveColumnInfo extends AlterTableInfo {
  removed_column: string;
  if_column_exists: boolean;
  cascade: boolean;
}

export interface ChangeColumnTypeInfo extends AlterTableInfo {
  column_name: string;
  target_type: LogicalType;
  expression: ParsedExpression;
}

export interface SetDefaultInfo extends AlterTableInfo {
  column_name: string;
  expression?: ParsedExpression;
}

export enum AlterForeignKeyType {
  AFT_ADD = 'AFT_ADD',
  AFT_DELETE = 'AFT_DELETE',
}

export interface AlterForeignKeyInfo extends AlterTableInfo {
  fk_table: string;
  pk_columns: string[];
  fk_columns: string[];
  pk_keys: PhysicalIndex[];
  fk_keys: PhysicalIndex[];
  alter_fk_type: AlterForeignKeyType;
}

export interface SetNotNullInfo extends AlterTableInfo {
  column_name: string;
}

export interface DropNotNullInfo extends AlterTableInfo {
  column_name: string;
}

export enum AlterViewType {
  INVALID = 'INVALID',
  RENAME_VIEW = 'RENAME_VIEW',
}

export interface AlterViewInfo extends AlterInfo {
  alter_view_type: AlterViewType;
}

export interface RenameViewInfo extends AlterViewInfo {
  new_view_name: string;
}

export interface AttachInfo extends ParseInfo {
  name: string;
  path: string;
  options: { [key: string]: Value };
}

export interface CopyInfo extends ParseInfo {
  catalog: string;
  schema: string;
  table: string;
  select_list: string[];
  is_from: boolean;
  format: string;
  file_path: string;
  options: { [key: string]: Value[] };
}

export interface DetachInfo extends ParseInfo {
  name: string;
  if_not_found: OnEntryNotFound;
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

export interface DropInfo extends ParseInfo {
  type: CatalogType;
  catalog: string;
  schema: string;
  name: string;
  if_not_found: OnEntryNotFound;
  cascade: boolean;
  allow_drop_internal: boolean;
}

export enum LoadType {
  LOAD = 'LOAD',
  INSTALL = 'INSTALL',
  FORCE_INSTALL = 'FORCE_INSTALL',
}

export interface LoadInfo extends ParseInfo {
  filename: string;
  load_type: LoadType;
  repository: string;
}

export interface PragmaInfo extends ParseInfo {
  name: string;
  parameters: Value[];
  named_parameters: { [key: string]: Value };
}

export enum TransactionType {
  INVALID = 'INVALID',
  BEGIN_TRANSACTION = 'BEGIN_TRANSACTION',
  COMMIT = 'COMMIT',
  ROLLBACK = 'ROLLBACK',
}

export interface TransactionInfo extends ParseInfo {
  type: TransactionType;
}

export interface VacuumInfo extends ParseInfo {
  options: any;
}
