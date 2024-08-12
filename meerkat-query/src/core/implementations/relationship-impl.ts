import { Field } from '../interfaces/field';
import { Relationship, RelationshipType } from '../interfaces/relationship';
import { Table } from '../interfaces/table';

export class RelationshipImpl implements Relationship {
  constructor(
    public id: string,
    public name: string,
    public type: RelationshipType,
    public sourceTable: Table,
    public targetTable: Table,
    public sourceField: Field,
    public targetField: Field
  ) {}

  getOtherTable(table: Table): Table {
    return table === this.sourceTable ? this.targetTable : this.sourceTable;
  }

  getOtherField(field: Field): Field {
    return field === this.sourceField ? this.targetField : this.sourceField;
  }

  involvesTable(table: Table): boolean {
    return table === this.sourceTable || table === this.targetTable;
  }

  involvesField(field: Field): boolean {
    return field === this.sourceField || field === this.targetField;
  }
}
