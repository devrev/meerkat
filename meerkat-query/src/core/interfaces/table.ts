import { VisibilityType } from '../enums/visibility-type';
import { Field } from './field';
import { Relationship } from './relationship';

export interface Table {
  id: string;
  databaseId: number;
  schema: string;
  name: string;
  displayName: string;
  description: string | null;
  entityType: string | null;
  active: boolean;
  visibilityType: VisibilityType;
  fields: Field[];
  relationships: Relationship[];

  /**
   * Retrieves a field by its name.
   */
  getFieldByName(name: string): Field | undefined;

  /**
   * Retrieves the primary key field of the table.
   */
  getPrimaryKey(): Field | undefined;

  /**
   * Retrieves all foreign key fields of the table.
   */
  getForeignKeys(): Field[];

  /**
   * Adds a new relationship to the table.
   */
  addRelationship(relationship: Relationship): void;

  /**
   * Retrieves all tables related to this table.
   */
  getRelatedTables(): Table[];

  /**
   * Generates the fully qualified name of the table (schema.tableName).
   */
  getFullyQualifiedName(): string;

  /**
   * Checks if the table has a specific field.
   */
  hasField(fieldName: string): boolean;

  /**
   * List all fields of the table.
   */
  listFields(): Field[];
}
