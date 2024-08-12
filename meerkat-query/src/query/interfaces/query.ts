export interface StockMeasure {
  type: 'count' | 'sum' | 'avg' | 'min' | 'max';
  fieldId: string;
}

export interface CustomMeasure {
  type: 'custom';
  expression: string;
  name: string;
}

export type Measure = StockMeasure | CustomMeasure;

export interface Dimension {
  fieldId: string;
}

export interface StockSelection {
  type: 'field';
  fieldId: string;
}

export interface CustomSelection {
  type: 'custom';
  expression: string;
  name: string;
}

export type Selection = StockSelection | CustomSelection;

export interface JoinCondition {
  leftFieldId: string;
  rightFieldId: string;
  operator: '=' | '<' | '>' | '<=' | '>=' | '!=' | 'IN';
}

export interface Join {
  sourceId: string;
  sourceType: 'table' | 'models';
  targetId: string;
  targetType: 'table' | 'models';
  type: 'left' | 'inner' | 'right' | 'full';
  conditions: JoinCondition[];
  alias?: string;
}

export interface OrderBy {
  fieldId: string;
  direction: 'asc' | 'desc';
}

export interface QueryBuilder {
  sourceId: string;
  sourceType: 'table' | 'models' | 'metric';

  /**
   * Measures / Aggregates
   */
  measures: Measure[];

  /**
   * Dimensions / Group by
   */
  dimensions: Dimension[];

  /**
   * Selections / Projections
   */
  selections: Selection[];

  /**
   * Joins
   */
  joins: Join[];

  //   /**
  //    * Filters
  //    */
  //   filters?: ;

  /**
   * Order By
   */
  orderBy?: OrderBy[];

  /**
   * Limit
   */
  limit?: number;

  /**
   * Offset
   */
  offset?: number;
}
