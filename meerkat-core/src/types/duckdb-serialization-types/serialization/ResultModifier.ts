import { BoundOrderByNode, OrderByNode } from './Nodes';
import { ParsedExpression } from './ParsedExpression';

export enum ResultModifierType {
  LIMIT_MODIFIER = 'LIMIT_MODIFIER',
  ORDER_MODIFIER = 'ORDER_MODIFIER',
  DISTINCT_MODIFIER = 'DISTINCT_MODIFIER',
  LIMIT_PERCENT_MODIFIER = 'LIMIT_PERCENT_MODIFIER',
}

export interface BaseResultModifier {
  type: ResultModifierType;
}

export type ResultModifier = LimitModifier | OrderModifier | DistinctModifier | LimitPercentModifier;

export interface LimitModifier extends BaseResultModifier {
  limit?: ParsedExpression;
  offset?: ParsedExpression;
}

export interface DistinctModifier extends BaseResultModifier {
  distinct_on_targets: ParsedExpression[];
}

export interface OrderModifier extends BaseResultModifier {
  orders: OrderByNode[];
}

export interface LimitPercentModifier extends BaseResultModifier {
  limit?: ParsedExpression;
  offset?: ParsedExpression;
}

export interface BoundOrderModifier {
  orders: BoundOrderByNode[];
}
