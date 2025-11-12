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

export interface ResolutionConfig {
  columnConfigs: ResolutionColumnConfig[];
  tableSchemas: TableSchema[];
}

export const BASE_DATA_SOURCE_NAME = '__base_query';
