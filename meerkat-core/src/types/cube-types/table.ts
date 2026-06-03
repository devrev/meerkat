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
  alias?: string;
  /**
   * Origin table name. Set internally by `getCombinedTableSchema` when
   * flat-mapping members from multiple tables into the merged schema, so
   * downstream lookups can disambiguate when two tables expose members of
   * the same `name` (e.g. `count(id)` exposed as `id___function__count` on
   * each side of a join). Not part of the public schema-author contract.
   */
  __sourceTable?: string;
};

export type Dimension = {
  name: string;
  sql: string;
  type: DimensionType;
  modifier?: {
    shouldUnnestGroupBy?: boolean;
    shouldFlattenArray?: boolean;
  };
  alias?: string;
  /** See {@link Measure.__sourceTable}. */
  __sourceTable?: string;
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
