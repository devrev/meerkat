import { ParsedExpression } from './ParsedExpression';

export enum ConstraintType {
  INVALID = 'INVALID',
  NOT_NULL = 'NOT_NULL',
  CHECK = 'CHECK',
  UNIQUE = 'UNIQUE',
  FOREIGN_KEY = 'FOREIGN_KEY',
}

export enum ForeignKeyType {
  FK_TYPE_PRIMARY_KEY_TABLE = 'FK_TYPE_PRIMARY_KEY_TABLE',
  FK_TYPE_FOREIGN_KEY_TABLE = 'FK_TYPE_FOREIGN_KEY_TABLE',
  FK_TYPE_SELF_REFERENCE_TABLE = 'FK_TYPE_SELF_REFERENCE_TABLE',
}

export interface BaseConstraint {
  type: ConstraintType;
}

export type Constraint = NotNullConstraint | CheckConstraint | ForeignKeyConstraint | UniqueConstraint;

export interface LogicalIndex {
  index: number;
}

export interface PhysicalIndex {
  index: number;
}

export interface NotNullConstraint extends BaseConstraint {
  enum: 'NOT_NULL';
  index: LogicalIndex;
}

export interface CheckConstraint extends BaseConstraint {
  enum: 'CHECK';
  expression: ParsedExpression;
}

export interface ForeignKeyConstraint extends BaseConstraint {
  enum: 'FOREIGN_KEY';
  pk_columns: string[];
  fk_columns: string[];
  fk_type: ForeignKeyType;
  schema: string;
  table: string;
  pk_keys: PhysicalIndex[];
  fk_keys: PhysicalIndex[];
}

export interface UniqueConstraint extends BaseConstraint {
  enum: 'UNIQUE';
  is_primary_key: boolean;
  index: LogicalIndex;
  columns: string[];
}
