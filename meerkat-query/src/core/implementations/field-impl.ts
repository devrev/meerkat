import { VisibilityType } from '../enums/visibility-type';
import { Field, FieldType, SemanticType } from '../interfaces/field';

interface FieldImplOptions {
  id: number;
  tableId: number;
  name: string;
  displayName: string;
  description: string | null;
  baseType: FieldType;
  semanticType: SemanticType | null;
  active: boolean;
  visibilityType: VisibilityType;
  tableName: string;
}

export class FieldImpl implements Field {
  public id: number;
  public tableId: number;
  public name: string;
  public displayName: string;
  public description: string | null;
  public baseType: FieldType;
  public semanticType: SemanticType | null;
  public active: boolean;
  public visibilityType: VisibilityType;
  public referenceName: string;
  public tableName: string;

  constructor(options: FieldImplOptions) {
    this.id = options.id;
    this.tableId = options.tableId;
    this.name = options.name;
    this.displayName = options.displayName;
    this.description = options.description;
    this.baseType = options.baseType;
    this.semanticType = options.semanticType;
    this.active = options.active;
    this.visibilityType = options.visibilityType;
    this.referenceName = `${options.tableName}.${this.name}`;
  }

  isNumeric(): boolean {
    return (
      this.baseType === FieldType.INTEGER || this.baseType === FieldType.FLOAT
    );
  }

  isDateType(): boolean {
    return (
      this.baseType === FieldType.DATE || this.baseType === FieldType.DATETIME
    );
  }

  isPrimaryKey(): boolean {
    return this.semanticType === SemanticType.TYPE_PK;
  }

  isForeignKey(): boolean {
    return this.semanticType === SemanticType.TYPE_FK;
  }
}
