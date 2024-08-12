import { VisibilityType } from '../enums/visibility-type';

/**
 * Enum representing the base data types of fields.
 */
export enum FieldType {
  INTEGER = 'INTEGER',
  FLOAT = 'FLOAT',
  STRING = 'STRING',
  BOOLEAN = 'BOOLEAN',
  DATE = 'DATE',
  DATETIME = 'DATETIME',
  JSON = 'JSON',
  ARRAY = 'ARRAY',
}

/**
 * Enum representing the semantic types of fields.
 */
export enum SemanticType {
  TYPE_PK = 'TYPE_PK',
  TYPE_FK = 'TYPE_FK',
  TYPE_CATEGORY = 'TYPE_CATEGORY',
  TYPE_METRIC = 'TYPE_METRIC',
  TYPE_DIMENSION = 'TYPE_DIMENSION',
  TYPE_ENTITY = 'TYPE_ENTITY',
}

export interface Field {
  id: number;
  tableId: number;
  name: string;
  displayName: string;
  description: string | null;
  baseType: FieldType;
  semanticType: SemanticType | null;
  active: boolean;
  visibilityType: VisibilityType;

  /**
   * Checks if the field is of a numeric type.
   */
  isNumeric(): boolean;

  /**
   * Checks if the field is of a date or datetime type.
   */
  isDateType(): boolean;

  /**
   * Checks if the field is a primary key.
   */
  isPrimaryKey(): boolean;

  /**
   * Checks if the field is a foreign key.
   */
  isForeignKey(): boolean;
}
