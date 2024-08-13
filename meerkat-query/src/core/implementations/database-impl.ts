import { v4 as uuidv4 } from 'uuid';
import { Model } from '../interfaces/model';
import { Relationship, RelationshipType } from '../interfaces/relationship';
import { Table } from '../interfaces/table';
import { RelationshipImpl } from './relationship-impl';

interface DatabaseImplOptions {
  id: number;
  name: string;
}

export class DatabaseImpl {
  private tables: Map<string, Table | Model> = new Map();
  public id: number;
  public name: string;

  constructor(options: DatabaseImplOptions) {
    this.id = options.id;
    this.name = options.name;
  }

  addTable(table: Table | Model): void {
    this.tables.set(table.id, table);
  }

  getTable(id: string): Table | Model | undefined {
    return this.tables.get(id);
  }

  getTableByName(name: string): Table | Model | undefined {
    return Array.from(this.tables.values()).find(
      (table) => table.name === name
    );
  }

  getAllTables(): (Table | Model)[] {
    return Array.from(this.tables.values());
  }

  removeTable(id: string): boolean {
    return this.tables.delete(id);
  }

  createRelationship(
    sourceTableId: string,
    targetTableId: string,
    sourceFieldName: string,
    targetFieldName: string,
    type: RelationshipType
  ): Relationship | null {
    const sourceTable = this.getTable(sourceTableId);
    const targetTable = this.getTable(targetTableId);

    if (!sourceTable || !targetTable) {
      console.error('Source or target table not found');
      return null;
    }

    const sourceField = sourceTable.getFieldByName(sourceFieldName);
    const targetField = targetTable.getFieldByName(targetFieldName);

    if (!sourceField || !targetField) {
      console.error('Source or target field not found');
      return null;
    }

    const relationship = new RelationshipImpl({
      id: uuidv4(),
      name: `${sourceTable.name}_${sourceField.name}_${targetTable.name}_${targetField.name}`,
      type,
      sourceTable,
      targetTable,
      sourceField,
      targetField,
    });

    sourceTable.addRelationship(relationship);
    targetTable.addRelationship(relationship);

    return relationship;
  }

  getRelatedTables(tableId: string): (Table | Model)[] {
    const table = this.getTable(tableId);
    return table ? table.getRelatedTables() : [];
  }

  getFieldCount(): number {
    return Array.from(this.tables.values()).reduce(
      (total, table) => total + table.fields.length,
      0
    );
  }
}
