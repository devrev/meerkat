import { PhysicalIndex } from './Constraint';
import { CreateInfo } from './CreateInfo';
import { Expression } from './Expression';
import { ColumnDataCollection, Value } from './Misc';
import {
  BoundOrderByNode,
  BoundPivotInfo,
  ColumnBinding,
  CTEMaterialize,
  JoinCondition,
  LogicalType,
  SampleOptions,
} from './Nodes';
import { ParseInfo } from './ParseInfo';
import { BoundOrderModifier } from './ResultModifier';
import { JoinType } from './TableRef';

export interface BaseLogicalOperator {
  id?: number;
  name?: string;
  type: string;
  children?: LogicalOperator[];
}

export type LogicalOperator =
  | LogicalProjection
  | LogicalFilter
  | LogicalAggregate
  | LogicalWindow
  | LogicalUnnest
  | LogicalLimit
  | LogicalOrder
  | LogicalTopN
  | LogicalDistinct
  | LogicalSample
  | LogicalLimitPercent
  | LogicalColumnDataGet
  | LogicalDelimGet
  | LogicalExpressionGet
  | LogicalDummyScan
  | LogicalEmptyResult
  | LogicalCTERef
  | LogicalComparisonJoin
  | LogicalAnyJoin
  | LogicalCrossProduct
  | LogicalPositionalJoin
  | LogicalSetOperation
  | LogicalRecursiveCTE
  | LogicalMaterializedCTE
  | LogicalInsert
  | LogicalDelete
  | LogicalUpdate
  | LogicalCreateTable
  | LogicalCreate
  | LogicalExplain
  | LogicalShow
  | LogicalSet
  | LogicalReset
  | LogicalSimple
  | LogicalPivot
  | LogicalGet
  | LogicalCopyToFile
  | LogicalCreateIndex
  | LogicalExtensionOperator;

export interface LogicalProjection extends BaseLogicalOperator {
  table_index: number;
  expressions: Expression[];
}

export interface LogicalFilter extends BaseLogicalOperator {
  expressions: Expression[];
  projection_map: number[];
}

export interface LogicalAggregate extends BaseLogicalOperator {
  expressions: Expression[];
  group_index: number;
  aggregate_index: number;
  groupings_index: number;
  groups: Expression[];
  grouping_sets: Set<number>[];
  grouping_functions: number[][];
}

export interface LogicalWindow extends BaseLogicalOperator {
  window_index: number;
  expressions: Expression[];
}

export interface LogicalUnnest extends BaseLogicalOperator {
  unnest_index: number;
  expressions: Expression[];
}

export interface LogicalLimit extends BaseLogicalOperator {
  limit_val: number;
  offset_val: number;
  limit?: Expression;
  offset?: Expression;
}

export interface LogicalOrder extends BaseLogicalOperator {
  orders: BoundOrderByNode[];
  projections: number[];
}

export interface LogicalTopN extends BaseLogicalOperator {
  orders: BoundOrderByNode[];
  limit: number;
  offset: number;
}

export enum DistinctType {
  DISTINCT = 'DISTINCT',
  DISTINCT_ON = 'DISTINCT_ON',
}

export interface LogicalDistinct extends BaseLogicalOperator {
  distinct_type: DistinctType;
  distinct_targets: Expression[];
  order_by?: BoundOrderModifier;
}

export interface LogicalSample extends BaseLogicalOperator {
  sample_options: SampleOptions;
}

export interface LogicalLimitPercent extends BaseLogicalOperator {
  limit_percent: number;
  offset_val: number;
  limit?: Expression;
  offset?: Expression;
}

export interface LogicalColumnDataGet extends BaseLogicalOperator {
  table_index: number;
  chunk_types: LogicalType[];
  collection: ColumnDataCollection;
}

export interface LogicalDelimGet extends BaseLogicalOperator {
  table_index: number;
  chunk_types: LogicalType[];
}

export interface LogicalExpressionGet extends BaseLogicalOperator {
  table_index: number;
  expr_types: LogicalType[];
  expressions: Expression[][];
}

export interface LogicalDummyScan extends BaseLogicalOperator {
  table_index: number;
}

export interface LogicalEmptyResult extends BaseLogicalOperator {
  return_types: LogicalType[];
  bindings: ColumnBinding[];
}

export interface LogicalCTERef extends BaseLogicalOperator {
  table_index: number;
  cte_index: number;
  chunk_types: LogicalType[];
  bound_columns: string[];
  materialized_cte: CTEMaterialize;
}

export interface LogicalComparisonJoin extends BaseLogicalOperator {
  join_type: JoinType;
  mark_index: number;
  left_projection_map: number[];
  right_projection_map: number[];
  conditions: JoinCondition[];
  mark_types: LogicalType[];
  duplicate_eliminated_columns: Expression[];
}

export interface LogicalAnyJoin extends BaseLogicalOperator {
  join_type: JoinType;
  mark_index: number;
  left_projection_map: number[];
  right_projection_map: number[];
  condition: Expression;
}

export interface LogicalCrossProduct extends BaseLogicalOperator {}

export interface LogicalPositionalJoin extends BaseLogicalOperator {}

export interface LogicalSetOperation extends BaseLogicalOperator {
  table_index: number;
  column_count: number;
}

export interface LogicalRecursiveCTE extends BaseLogicalOperator {
  union_all: boolean;
  ctename: string;
  table_index: number;
  column_count: number;
}

export interface LogicalMaterializedCTE extends BaseLogicalOperator {
  table_index: number;
  column_count: number;
  ctename: string;
}

export enum OnConflictAction {
  THROW = 'THROW',
  NOTHING = 'NOTHING',
  UPDATE = 'UPDATE',
  REPLACE = 'REPLACE',
}

export interface LogicalInsert extends BaseLogicalOperator {
  catalog: string;
  schema: string;
  table: string;
  insert_values: Expression[][];
  column_index_map: number[];
  expected_types: LogicalType[];
  table_index: number;
  return_chunk: boolean;
  bound_defaults: Expression[];
  action_type: OnConflictAction;
  expected_set_types: LogicalType[];
  on_conflict_filter: Set<number>;
  on_conflict_condition?: Expression;
  do_update_condition?: Expression;
  set_columns: number[];
  set_types: LogicalType[];
  excluded_table_index: number;
  columns_to_fetch: number[];
  source_columns: number[];
}

export interface LogicalDelete extends BaseLogicalOperator {
  catalog: string;
  schema: string;
  table: string;
  table_index: number;
  return_chunk: boolean;
  expressions: Expression[];
}

export interface LogicalUpdate extends BaseLogicalOperator {
  catalog: string;
  schema: string;
  table: string;
  table_index: number;
  return_chunk: boolean;
  expressions: Expression[];
  columns: PhysicalIndex[];
  bound_defaults: Expression[];
  update_is_del_and_insert: boolean;
}

export interface LogicalCreateTable extends BaseLogicalOperator {
  catalog: string;
  schema: string;
  info: CreateInfo;
}

export interface LogicalCreate extends BaseLogicalOperator {
  info: CreateInfo;
}

export enum ExplainType {
  EXPLAIN_STANDARD = 'EXPLAIN_STANDARD',
  EXPLAIN_ANALYZE = 'EXPLAIN_ANALYZE',
}

export interface LogicalExplain extends BaseLogicalOperator {
  explain_type: ExplainType;
  physical_plan: string;
  logical_plan_unopt: string;
  logical_plan_opt: string;
}

export interface LogicalShow extends BaseLogicalOperator {
  types_select: LogicalType[];
  aliases: string[];
}

export enum SetScope {
  AUTOMATIC = 'AUTOMATIC',
  LOCAL = 'LOCAL',
  SESSION = 'SESSION',
  GLOBAL = 'GLOBAL',
}

export interface LogicalSet extends BaseLogicalOperator {
  name: string;
  value: Value;
  scope: SetScope;
}

export interface LogicalReset extends BaseLogicalOperator {
  name: string;
  scope: SetScope;
}

export interface LogicalSimple extends BaseLogicalOperator {
  info: ParseInfo;
}

export interface LogicalPivot extends BaseLogicalOperator {
  pivot_index: number;
  bound_pivot: BoundPivotInfo;
}

export interface LogicalGet extends BaseLogicalOperator {
  // Custom implementation, properties not defined
}

export interface LogicalCopyToFile extends BaseLogicalOperator {
  // Custom implementation, properties not defined
}

export interface LogicalCreateIndex extends BaseLogicalOperator {
  info: CreateInfo;
  unbound_expressions: Expression[];
}

export interface LogicalExtensionOperator extends BaseLogicalOperator {
  // Custom implementation, properties not defined
}
