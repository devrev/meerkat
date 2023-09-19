export type MeasureType = 'string' | 'time' | 'number' | 'boolean';

export type DimensionType = 'string' | 'time' | 'number' | 'boolean';

export type Measure = {
  sql: string;
  type: MeasureType;
};

export type Dimension = {
  sql: string;
  type: DimensionType;
};

export type TableSchema = {
  cube: string;
  measures: Measure[];
  dimensions: Dimension[];
  sql?: string;
};
