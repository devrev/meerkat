import { Field, StructRowProxy, Table, Type, Vector } from 'apache-arrow';
import _isNil from 'lodash/isNil';

export const convertArrowValueToJS = (field: Field, value: unknown): unknown => {
  if (_isNil(value)) return value;
  switch (field.typeId) {
    case Type.Null:
      return null;
    case Type.Date:
    case Type.Timestamp:
      return new Date(value as number).toISOString();
    case Type.Float:
    case Type.Float16:
    case Type.Float32:
    case Type.Float64:
      return value;
    case Type.Int:
    case Type.Int8:
    case Type.Int16:
    case Type.Int32:
    case Type.Int64:
    case Type.Uint8:
    case Type.Uint16:
    case Type.Uint32:
    case Type.Uint64:
      return parseInt((value as object).toString());
    case Type.Decimal:
      return parseFloat((value as object).toString());
    case Type.List:
      if (field.type.children[0]) {
        const array: unknown[] = [];
        (value as Vector)
          .toArray()
          .forEach((v: unknown) => array.push(convertArrowValueToJS(field.type.children[0], v)));
        return array;
      }
      return [];
    default:
      return value;
  }
};

export const convertArrowTableToJSON = (table: Table): Record<string, unknown>[] => {
  return table
    .toArray()
    .map((row: StructRowProxy) => row.toJSON())
    .map((datum: Record<string, unknown>) => {
      return table.schema.fields.reduce((acc, schemaField) => {
        acc[schemaField.name] = convertArrowValueToJS(schemaField, datum[schemaField.name]);
        return acc;
      }, {} as Record<string, unknown>);
    });
};
