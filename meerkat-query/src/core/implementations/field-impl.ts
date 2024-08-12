import { VisibilityType } from '../enums/visibility-type';
import { Field, FieldType, SemanticType } from '../interfaces/field';

export class FieldImpl implements Field {
  constructor(
    public id: number,
    public tableId: number,
    public name: string,
    public displayName: string,
    public description: string | null,
    public baseType: FieldType,
    public semanticType: SemanticType | null,
    public active: boolean,
    public visibilityType: VisibilityType
  ) {}

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
