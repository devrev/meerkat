import {
  DimensionType,
  MeasureType,
  TableSchema,
} from '../types/cube-types/table';

export interface ResolutionColumnConfig {
  // Name of the column that needs resolution.
  // Should match a measure or dimension in the query.
  name: string;
  // Type of the dimension/measure (e.g., 'string', 'number', 'string_array')
  // Used to determine if special array handling (UNNEST/ARRAY_AGG) is needed.
  type: DimensionType | MeasureType;
  // Name of the data source to use for resolution.
  source: string;
  // Name of the column in the data source to join on.
  joinColumn: string;
  // Columns from the source table that should be included for resolution.
  resolutionColumns: string[];
}

export interface SqlOverrideConfig {
  // Name of the field that needs SQL override.
  // Should match a measure or dimension in the table schema.
  fieldName: string;
  // Override SQL expression (e.g., CASE WHEN {{FIELD}}=1 THEN 'P0' WHEN {{FIELD}}=2 THEN 'P1' END)
  overrideSql: string;
  // Type of the transformed field after override (e.g., 'string')
  type: DimensionType | MeasureType;
}

export interface ResolutionConfig {
  columnConfigs: ResolutionColumnConfig[];
  tableSchemas: TableSchema[];
  // Optional: SQL overrides to apply after base SQL generation
  sqlOverrideConfigs?: SqlOverrideConfig[];
}

export const BASE_DATA_SOURCE_NAME = '__base_query';
