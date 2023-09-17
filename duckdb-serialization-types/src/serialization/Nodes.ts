import { Expression, ExpressionType } from './Expression';
import { Value } from './Misc';
import { ParsedExpression } from './ParsedExpression';
import { SelectStatement } from './Statement';
import { TableFilter } from './TableFilter';
import { ExtraTypeInfo } from './Types';

export interface LogicalType {
  id?: number;
  type_info?: ExtraTypeInfo;
}

export enum CTEMaterialize {
  CTE_MATERIALIZE_DEFAULT = 'CTE_MATERIALIZE_DEFAULT',
  CTE_MATERIALIZE_ALWAYS = 'CTE_MATERIALIZE_ALWAYS',
  CTE_MATERIALIZE_NEVER = 'CTE_MATERIALIZE_NEVER',
}

export interface CommonTableExpressionInfo {
  aliases: string[];
  query: SelectStatement;
  materialized: CTEMaterialize;
}

export interface CommonTableExpressionMap {
  map: Record<string, CommonTableExpressionInfo>;
}

export enum OrderType {
  INVALID = 'INVALID',
  ORDER_DEFAULT = 'ORDER_DEFAULT',
  ASCENDING = 'ASCENDING',
  DESCENDING = 'DESCENDING',
}

export enum OrderByNullType {
  INVALID = 'INVALID',
  ORDER_DEFAULT = 'ORDER_DEFAULT',
  NULLS_FIRST = 'NULLS_FIRST',
  NULLS_LAST = 'NULLS_LAST',
}

export enum DefaultOrderByNullType {
  INVALID = 'INVALID',
  NULLS_FIRST = 'NULLS_FIRST',
  NULLS_LAST = 'NULLS_LAST',
  NULLS_FIRST_ON_ASC_LAST_ON_DESC = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC',
  NULLS_LAST_ON_ASC_FIRST_ON_DESC = 'NULLS_LAST_ON_ASC_FIRST_ON_DESC',
}

export interface OrderByNode {
  type: OrderType;
  null_order: OrderByNullType;
  expression: ParsedExpression;
}

export interface BoundOrderByNode {
  type: OrderType;
  null_order: OrderByNullType;
  expression: Expression;
}

export interface CaseCheck {
  when_expr: ParsedExpression;
  then_expr: ParsedExpression;
}

export interface BoundCaseCheck {
  when_expr: Expression;
  then_expr: Expression;
}

export enum SampleMethod {
  SYSTEM_SAMPLE = 'SYSTEM_SAMPLE',
  BERNOULLI_SAMPLE = 'BERNOULLI_SAMPLE',
  RESERVOIR_SAMPLE = 'RESERVOIR_SAMPLE',
}

export interface SampleOptions {
  sample_size: Value;
  is_percentage: boolean;
  method: SampleMethod;
  seed: number;
}

export interface PivotColumn {
  pivot_expressions: ParsedExpression[];
  unpivot_names: string[];
  entries: PivotColumnEntry[];
  pivot_enum: string;
}

export interface PivotColumnEntry {
  values: Value[];
  star_expr?: ParsedExpression;
  alias: string;
}

export interface BoundPivotInfo {
  group_count: number;
  types: LogicalType[];
  pivot_values: string[];
  aggregates: Expression[];
}

export enum TableColumnType {
  STANDARD = 'STANDARD',
  GENERATED = 'GENERATED',
}

export enum CompressionType {
  COMPRESSION_AUTO = 'COMPRESSION_AUTO',
  COMPRESSION_UNCOMPRESSED = 'COMPRESSION_UNCOMPRESSED',
  COMPRESSION_CONSTANT = 'COMPRESSION_CONSTANT',
  COMPRESSION_RLE = 'COMPRESSION_RLE',
  COMPRESSION_DICTIONARY = 'COMPRESSION_DICTIONARY',
  COMPRESSION_PFOR_DELTA = 'COMPRESSION_PFOR_DELTA',
  COMPRESSION_BITPACKING = 'COMPRESSION_BITPACKING',
  COMPRESSION_FSST = 'COMPRESSION_FSST',
  COMPRESSION_CHIMP = 'COMPRESSION_CHIMP',
  COMPRESSION_PATAS = 'COMPRESSION_PATAS',
  COMPRESSION_COUNT = 'COMPRESSION_COUNT',
}

export interface ColumnDefinition {
  name: string;
  type: LogicalType;
  expression?: ParsedExpression;
  category: TableColumnType;
  compression_type: CompressionType;
}

export interface ColumnList {
  columns: ColumnDefinition[];
}

export interface ColumnBinding {
  table_index: number;
  column_index: number;
}

export interface BoundParameterData {
  value: Value;
  return_type: LogicalType;
}

export interface JoinCondition {
  left: Expression;
  right: Expression;
  comparison: ExpressionType;
}

export interface VacuumOptions {
  vacuum: boolean;
  analyze: boolean;
}

export interface TableFilterSet {
  filters: Record<number, TableFilter>;
}

export interface MultiFileReaderOptions {
  filename: boolean;
  hive_partitioning: boolean;
  auto_detect_hive_partitioning: boolean;
  union_by_name: boolean;
  hive_types_autocast: boolean;
  hive_types_schema: Record<string, LogicalType>;
}

export interface MultiFileReaderBindData {
  filename_idx: number;
  hive_partitioning_indexes: HivePartitioningIndex[];
}

export interface HivePartitioningIndex {
  value: string;
  index: number;
}

enum FileCompressionType {
  AUTO_DETECT = 'AUTO_DETECT',
  UNCOMPRESSED = 'UNCOMPRESSED',
  GZIP = 'GZIP',
  ZSTD = 'ZSTD',
}

export interface CSVReaderOptions {
  has_delimiter: boolean;
  has_quote: boolean;
  has_escape: boolean;
  has_header: boolean;
  ignore_errors: boolean;
  buffer_sample_size: number;
  null_str: string;
  compression: FileCompressionType;
  allow_quoted_nulls: boolean;
  skip_rows_set: boolean;
  maximum_line_size: number;
  normalize_names: boolean;
  force_not_null: boolean[];
  all_varchar: boolean;
  sample_chunk_size: number;
  sample_chunks: number;
  auto_detect: boolean;
  file_path: string;
  decimal_separator: string;
  null_padding: boolean;
  buffer_size: number;
  file_options: MultiFileReaderOptions;
  force_quote: boolean[];
  rejects_table_name: string;
  rejects_limit: number;
  rejects_recovery_columns: string[];
  rejects_recovery_column_ids: number[];
  dialect_options: any;
}

export interface StrpTimeFormat {
  format_specifier: string;
}

export interface ReadCSVData {
  files: string[];
  csv_types: LogicalType[];
  csv_names: string[];
  return_types: LogicalType[];
  return_names: string[];
  filename_col_idx: number;
  options: any;
  single_threaded: boolean;
  reader_bind: MultiFileReaderBindData;
  column_info: ColumnInfo[];
}

export interface ColumnInfo {
  names: string[];
  types: LogicalType[];
}

export interface IntervalT {
  months: number;
  days: number;
  micros: number;
}
