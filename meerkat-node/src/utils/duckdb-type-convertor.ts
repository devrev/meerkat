import {
  DuckDBListValue,
  DuckDBType,
  DuckDBTypeId,
  DuckDBValue,
} from '@duckdb/node-api';

export const convertDuckDBValueToJS = (
  field: DuckDBType,
  value: DuckDBValue
): unknown => {
  switch (field.typeId) {
    case DuckDBTypeId.SQLNULL:
      return null;
    case DuckDBTypeId.DATE:
    case DuckDBTypeId.TIMESTAMP:
    case DuckDBTypeId.TIME:
      return new Date(value as number).toISOString();
    case DuckDBTypeId.FLOAT:
    case DuckDBTypeId.DOUBLE:
      return value;
    case DuckDBTypeId.INTEGER:
    case DuckDBTypeId.TINYINT:
    case DuckDBTypeId.SMALLINT:
    case DuckDBTypeId.BIGINT:
    case DuckDBTypeId.UTINYINT:
    case DuckDBTypeId.USMALLINT:
    case DuckDBTypeId.UINTEGER:
    case DuckDBTypeId.UBIGINT:
      return parseInt((value as object).toString());
    case DuckDBTypeId.DECIMAL:
      return parseFloat((value as object).toString());
    case DuckDBTypeId.LIST: {
      if (!value) return [];
      const listValue = value as DuckDBListValue;
      return listValue.items.map((item) =>
        convertDuckDBValueToJS(field.valueType, item)
      );
    }
    default:
      return value;
  }
};

export const convertRecordDuckDBValueToJSON = (
  data: Record<string, DuckDBValue>[],
  columns: { name: string; type: DuckDBType }[]
): Record<string, unknown>[] => {
  return data.map((row: Record<string, DuckDBValue>) => {
    return columns.reduce((acc, column) => {
      acc[column.name] = convertDuckDBValueToJS(column.type, row[column.name]);
      return acc;
    }, {} as Record<string, unknown>);
  });
};
