export type MeasureType =
  | 'string'
  | 'string_array'
  | 'time'
  | 'number'
  | 'number_array'
  | 'boolean';

export type DimensionType =
  | 'string'
  | 'string_array'
  | 'time'
  | 'number'
  | 'number_array'
  | 'boolean';

export type Measure = {
  name: string;
  sql: string;
  type: MeasureType;
};

export type Dimension = {
  name: string;
  sql: string;
  type: DimensionType;
  modifier?: {
    unNestedGroupBy?: boolean
  }
};

export type Join = {
  sql: string;
};

export type TableSchema = {
  name: string;
  sql: string;
  measures: Measure[];
  dimensions: Dimension[];
  joins?: Join[];
};

export interface ContextParams {
  [key: string]: string;
}
