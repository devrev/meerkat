import { LogicalType } from './Nodes';

export enum ExtraTypeInfoType {
  INVALID_TYPE_INFO = 'INVALID_TYPE_INFO',
  GENERIC_TYPE_INFO = 'GENERIC_TYPE_INFO',
  DECIMAL_TYPE_INFO = 'DECIMAL_TYPE_INFO',
  STRING_TYPE_INFO = 'STRING_TYPE_INFO',
  LIST_TYPE_INFO = 'LIST_TYPE_INFO',
  STRUCT_TYPE_INFO = 'STRUCT_TYPE_INFO',
  ENUM_TYPE_INFO = 'ENUM_TYPE_INFO',
  USER_TYPE_INFO = 'USER_TYPE_INFO',
  AGGREGATE_STATE_TYPE_INFO = 'AGGREGATE_STATE_TYPE_INFO',
}

export interface BaseExtraTypeInfo {
  type: ExtraTypeInfoType;
  alias: string;
}

export type ExtraTypeInfo =
  | DecimalTypeInfo
  | StringTypeInfo
  | ListTypeInfo
  | StructTypeInfo
  | AggregateStateTypeInfo
  | UserTypeInfo
  | EnumTypeInfo
  | InvalidTypeInfo
  | GenericTypeInfo;

export interface DecimalTypeInfo extends BaseExtraTypeInfo {
  enum: 'DECIMAL_TYPE_INFO';
  width: number;
  scale: number;
}

export interface StringTypeInfo extends BaseExtraTypeInfo {
  enum: 'STRING_TYPE_INFO';
  collation: string;
}

export interface ListTypeInfo extends BaseExtraTypeInfo {
  enum: 'LIST_TYPE_INFO';
  child_type: LogicalType;
}

export interface StructTypeInfo extends BaseExtraTypeInfo {
  enum: 'STRUCT_TYPE_INFO';
  child_types: LogicalType[];
}

export interface AggregateStateTypeInfo extends BaseExtraTypeInfo {
  enum: 'AGGREGATE_STATE_TYPE_INFO';
  function_name: string;
  return_type: LogicalType;
  bound_argument_types: LogicalType[];
}

export interface UserTypeInfo extends BaseExtraTypeInfo {
  enum: 'USER_TYPE_INFO';
  user_type_name: string;
}

export interface EnumTypeInfo extends BaseExtraTypeInfo {
  enum: 'ENUM_TYPE_INFO';
  // custom_implementation: true; // Not sure how this translates to TypeScript
}

export interface InvalidTypeInfo extends BaseExtraTypeInfo {
  enum: 'INVALID_TYPE_INFO';
  // custom_switch_code: 'return nullptr;'; // Not sure how this translates to TypeScript
}

export interface GenericTypeInfo extends BaseExtraTypeInfo {
  enum: 'GENERIC_TYPE_INFO';
  // custom_switch_code: 'result = make_shared<ExtraTypeInfo>(type);\nbreak;'; // Not sure how this translates to TypeScript
}
