import { ExpressionType } from './Expression';
import { Value } from './Misc';

export enum TableFilterType {
  CONSTANT_COMPARISON = 'CONSTANT_COMPARISON',
  IS_NULL = 'IS_NULL',
  IS_NOT_NULL = 'IS_NOT_NULL',
  CONJUNCTION_OR = 'CONJUNCTION_OR',
  CONJUNCTION_AND = 'CONJUNCTION_AND',
}

export interface BaseTableFilter {
  filter_type: TableFilterType;
}

export type TableFilter = ConstantFilter | IsNullFilter | IsNotNullFilter | ConjunctionOrFilter | ConjunctionAndFilter;

export interface IsNullFilter extends BaseTableFilter {
  filter_type: TableFilterType.IS_NULL;
}

export interface IsNotNullFilter extends BaseTableFilter {
  filter_type: TableFilterType.IS_NOT_NULL;
}

export interface ConstantFilter extends BaseTableFilter {
  filter_type: TableFilterType.CONSTANT_COMPARISON;
  comparison_type: ExpressionType;
  constant: Value;
}

export interface ConjunctionOrFilter extends BaseTableFilter {
  filter_type: TableFilterType.CONJUNCTION_OR;
  child_filters: TableFilter[];
}

export interface ConjunctionAndFilter extends BaseTableFilter {
  filter_type: TableFilterType.CONJUNCTION_AND;
  child_filters: TableFilter[];
}
