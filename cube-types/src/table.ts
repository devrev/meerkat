export type MeasureType = 'string' | 'time' | 'number' | 'boolean';

export type DimensionType = 'string' | 'time' | 'number' | 'boolean';

export type Measure = {
  name: string;
  sql: string;
  type: MeasureType;
};

export type Dimension = {
  name: string;
  sql: string;
  type: DimensionType;
};

export type TableSchema = {
  name: string;
  cube: string;
  measures: Measure[];
  dimensions: Dimension[];
  sql?: string;
};
