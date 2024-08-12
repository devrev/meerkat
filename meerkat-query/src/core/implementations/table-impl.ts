import { VisibilityType } from '../enums/visibility-type';
import { Field } from '../interfaces/field';
import { Relationship } from '../interfaces/relationship';
import { Table } from '../interfaces/table';

export class TableImpl implements Table {
  constructor(
    public id: string,
    public databaseId: number,
    public schema: string,
    public name: string,
    public displayName: string,
    public description: string | null,
    public entityType: string | null,
    public active: boolean,
    public visibilityType: VisibilityType,
    public fields: Field[],
    public relationships: Relationship[]
  ) {}

  getFieldByName(name: string): Field | undefined {
    return this.fields.find((field) => field.name === name);
  }

  getPrimaryKey(): Field | undefined {
    return this.fields.find((field) => field.isPrimaryKey());
  }

  getForeignKeys(): Field[] {
    return this.fields.filter((field) => field.isForeignKey());
  }

  addRelationship(relationship: Relationship): void {
    this.relationships.push(relationship);
  }

  getRelatedTables(): Table[] {
    return this.relationships.map((rel) => rel.getOtherTable(this));
  }

  getFullyQualifiedName(): string {
    return `${this.schema}.${this.name}`;
  }

  hasField(fieldName: string): boolean {
    return this.fields.some((field) => field.name === fieldName);
  }

  removeRelationship(relationshipId: string): boolean {
    const index = this.relationships.findIndex(
      (rel) => rel.id === relationshipId
    );
    if (index !== -1) {
      this.relationships.splice(index, 1);
      return true;
    }
    return false;
  }

  listFields(): Field[] {
    return this.fields;
  }
}
