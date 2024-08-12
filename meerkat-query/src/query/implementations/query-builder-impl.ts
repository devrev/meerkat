import {
  CustomMeasure,
  CustomSelection,
  Dimension,
  Join,
  JoinCondition,
  Measure,
  OrderBy,
  QueryBuilder,
  Selection,
  StockMeasure,
  StockSelection,
} from '../interfaces/query';

export class QueryBuilderImpl implements QueryBuilder {
  sourceId: string;
  sourceType: 'table' | 'models' | 'metric';
  measures: Measure[];
  dimensions: Dimension[];
  selections: Selection[];
  joins: Join[];
  orderBy?: OrderBy[];
  limit?: number;
  offset?: number;

  constructor(params: {
    sourceId: string;
    sourceType: 'table' | 'models' | 'metric';
    measures?: Measure[];
    dimensions?: Dimension[];
    selections?: Selection[];
    joins?: Join[];
    orderBy?: OrderBy[];
    limit?: number;
    offset?: number;
  }) {
    this.sourceId = params.sourceId;
    this.sourceType = params.sourceType;
    this.measures = params.measures || [];
    this.dimensions = params.dimensions || [];
    this.selections = params.selections || [];
    this.joins = params.joins || [];
    this.orderBy = params.orderBy;
    this.limit = params.limit;
    this.offset = params.offset;
  }

  addMeasure(measure: Measure): this {
    this.measures.push(measure);
    return this;
  }

  addDimension(dimension: Dimension): this {
    this.dimensions.push(dimension);
    return this;
  }

  addSelection(selection: Selection): this {
    this.selections.push(selection);
    return this;
  }

  addJoin(join: Join): this {
    this.joins.push(join);
    return this;
  }

  setOrderBy(orderBy: OrderBy[]): this {
    this.orderBy = orderBy;
    return this;
  }

  setLimit(limit: number): this {
    this.limit = limit;
    return this;
  }

  setOffset(offset: number): this {
    this.offset = offset;
    return this;
  }

  // Helper methods for creating specific types
  static createStockMeasure(
    type: 'count' | 'sum' | 'avg' | 'min' | 'max',
    fieldId: string
  ): StockMeasure {
    return { type, fieldId };
  }

  static createCustomMeasure(expression: string, name: string): CustomMeasure {
    return { type: 'custom', expression, name };
  }

  static createDimension(fieldId: string): Dimension {
    return { fieldId };
  }

  static createStockSelection(fieldId: string): StockSelection {
    return { type: 'field', fieldId };
  }

  static createCustomSelection(
    expression: string,
    name: string
  ): CustomSelection {
    return { type: 'custom', expression, name };
  }

  static createJoinCondition(
    leftFieldId: string,
    rightFieldId: string,
    operator: '=' | '<' | '>' | '<=' | '>=' | '!=' | 'IN'
  ): JoinCondition {
    return { leftFieldId, rightFieldId, operator };
  }

  createJoin(
    sourceId: string,
    sourceType: 'table' | 'models',
    targetId: string,
    targetType: 'table' | 'models',
    type: 'left' | 'inner' | 'right' | 'full',
    conditions: JoinCondition[],
    alias?: string
  ): Join {
    return {
      sourceId,
      sourceType,
      targetId,
      targetType,
      type,
      conditions,
      alias,
    };
  }

  static createOrderBy(fieldId: string, direction: 'asc' | 'desc'): OrderBy {
    return { fieldId, direction };
  }

  // Method to get the final query object
  build(): QueryBuilder {
    return {
      sourceId: this.sourceId,
      sourceType: this.sourceType,
      measures: this.measures,
      dimensions: this.dimensions,
      selections: this.selections,
      joins: this.joins,
      orderBy: this.orderBy,
      limit: this.limit,
      offset: this.offset,
    };
  }

  // Method to create a new QueryBuilder from an existing Query
  static fromQuery(query: QueryBuilder): QueryBuilder {
    return new QueryBuilderImpl(query);
  }

  // Method to clone the current QueryBuilder
  clone(): QueryBuilder {
    return new QueryBuilderImpl(this.build());
  }
}
