import { Field } from '../interfaces/field';
import { Relationship, RelationshipType } from '../interfaces/relationship';
import { Table } from '../interfaces/table';

interface RelationshipImplOptions {
  id: string;
  name: string;
  type: RelationshipType;
  sourceTable: Table;
  targetTable: Table;
  sourceField: Field;
  targetField: Field;
}

export class RelationshipImpl implements Relationship {
  public id: string;
  public name: string;
  public type: RelationshipType;
  public sourceTable: Table;
  public targetTable: Table;
  public sourceField: Field;
  public targetField: Field;

  constructor(options: RelationshipImplOptions) {
    this.id = options.id;
    this.name = options.name;
    this.type = options.type;
    this.sourceTable = options.sourceTable;
    this.targetTable = options.targetTable;
    this.sourceField = options.sourceField;
    this.targetField = options.targetField;
  }

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
