import { Field } from './field';
import { Table } from './table';

/**
 * Enum representing the types of relationships between tables.
 */
export enum RelationshipType {
  ONE_TO_ONE = 'ONE_TO_ONE',
  ONE_TO_MANY = 'ONE_TO_MANY',
  MANY_TO_ONE = 'MANY_TO_ONE',
  MANY_TO_MANY = 'MANY_TO_MANY',
}

/**
 * Represents a relationship between two tables.
 */
export interface Relationship {
  id: string;
  name: string;
  type: RelationshipType;
  sourceTable: Table;
  targetTable: Table;
  sourceField: Field;
  targetField: Field;

  /**
   * Gets the table on the other side of the relationship from the given table.
   */
  getOtherTable(table: Table): Table;

  /**
   * Gets the field on the other side of the relationship from the given field.
   */
  getOtherField(field: Field): Field;

  /**
   * Checks if the relationship involves the given table.
   */
  involvesTable(table: Table): boolean;

  /**
   * Checks if the relationship involves the given field.
   */
  involvesField(field: Field): boolean;
}
