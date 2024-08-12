import {
  JoinNode,
  JoinPath,
  LogicalAndFilter,
  LogicalOrFilter,
  Member,
  Query,
  QueryFilter,
} from '@devrev/meerkat-core';
import {
  CustomMeasure,
  CustomSelection,
  Dimension,
  Join,
  Measure,
  OrderBy,
  QueryBuilder,
  StockMeasure,
  StockSelection,
} from '../interfaces/query';
interface SchemaDefinition {
  name: string;
  sql: string;
  measures: Array<{
    name: string;
    sql: string;
    type: string;
  }>;
  dimensions: Array<{
    name: string;
    sql: string;
    type: string;
  }>;
  joins?: Array<{
    sql: string;
  }>;
}

export function queryBuilderToCubeQuery(queryBuilder: QueryBuilder): {
  query: Query;
  schemas: SchemaDefinition[];
} {
  const cubeQuery: Query = {
    measures: [],
    dimensions: [],
    filters: [],
    joinPaths: [],
  };

  const schemas: { [key: string]: SchemaDefinition } = {};

  // Initialize the main schema
  schemas[queryBuilder.sourceId] = {
    name: queryBuilder.sourceId,
    sql: `select * from ${queryBuilder.sourceId}`,
    measures: [],
    dimensions: [],
    joins: [],
  };

  // Convert measures
  cubeQuery.measures = queryBuilder.measures.map((measure) =>
    convertMeasure(measure, schemas[queryBuilder.sourceId])
  );

  // Convert dimensions
  cubeQuery.dimensions = queryBuilder.dimensions.map((dimension) =>
    convertDimension(dimension, schemas)
  );

  // Convert joins to joinPaths and schema joins
  if (queryBuilder.joins && queryBuilder.joins.length > 0) {
    cubeQuery.joinPaths = convertJoins(queryBuilder.joins, schemas);
  }

  // Convert order by
  if (queryBuilder.orderBy && queryBuilder.orderBy.length > 0) {
    cubeQuery.order = convertOrderBy(queryBuilder.orderBy);
  }

  // Set limit and offset
  if (queryBuilder.limit !== undefined) cubeQuery.limit = queryBuilder.limit;
  if (queryBuilder.offset !== undefined) cubeQuery.offset = queryBuilder.offset;

  return { query: cubeQuery, schemas: Object.values(schemas) };
}

function convertMeasure(measure: Measure, schema: SchemaDefinition): Member {
  if (measure.type === 'custom') {
    const customMeasure = measure as CustomMeasure;
    schema.measures.push({
      name: measure.name,
      sql: customMeasure.expression,
      type: 'number', // Assuming custom measures are always numbers
    });
    return `${schema.name}.${measure.name}`;
  } else {
    const stockMeasure = measure as StockMeasure;
    const measureName = `${stockMeasure.fieldId}_${stockMeasure.type}`;
    schema.measures.push({
      name: measureName,
      sql: `${stockMeasure.type.toUpperCase()}(${stockMeasure.fieldId})`,
      type: 'number',
    });
    return `${schema.name}.${measureName}`;
  }
}

function convertDimension(
  dimension: Dimension,
  schemas: { [key: string]: SchemaDefinition }
): Member {
  const [tableName, fieldName] = dimension.fieldId.split('.');
  if (!schemas[tableName]) {
    schemas[tableName] = {
      name: tableName,
      sql: `select * from ${tableName}`,
      measures: [],
      dimensions: [],
      joins: [],
    };
  }
  schemas[tableName].dimensions.push({
    name: fieldName,
    sql: dimension.fieldId,
    type: 'string', // Assuming string type for simplicity, adjust if needed
  });
  return dimension.fieldId;
}

function convertSelection(
  selection: Selection
): QueryFilter | LogicalAndFilter | LogicalOrFilter {
  if (selection.type === 'field') {
    const stockSelection = selection as unknown as StockSelection;
    return {
      member: stockSelection.fieldId,
      operator: 'equals', // Default operator, adjust as needed
      values: [], // Values should be filled based on your requirements
    };
  } else {
    const customSelection = selection as unknown as CustomSelection;
    return {
      and: [
        {
          member: customSelection.expression,
          operator: 'equals',
          values: [],
        },
      ],
    };
  }
}

function convertJoins(
  joins: Join[],
  schemas: { [key: string]: SchemaDefinition }
): JoinPath[] {
  return joins.map((join) => {
    const joinNode: JoinNode = {
      left: join.sourceId,
      right: join.targetId,
      on: join.conditions.map((condition) => condition.leftFieldId).join(','),
    };

    // Ensure schemas exist for both source and target
    if (!schemas[join.sourceId]) {
      schemas[join.sourceId] = {
        name: join.sourceId,
        sql: `select * from ${join.sourceId}`,
        measures: [],
        dimensions: [],
        joins: [],
      };
    }
    if (!schemas[join.targetId]) {
      schemas[join.targetId] = {
        name: join.targetId,
        sql: `select * from ${join.targetId}`,
        measures: [],
        dimensions: [],
        joins: [],
      };
    }

    // Add join to source schema
    schemas[join.sourceId].joins!.push({
      sql: `${join.sourceId}.${join.conditions[0].leftFieldId} = ${join.targetId}.${join.conditions[0].rightFieldId}`,
    });

    return [joinNode];
  });
}

function convertOrderBy(orderBy: OrderBy[]): Record<string, 'asc' | 'desc'> {
  const order: Record<string, 'asc' | 'desc'> = {};
  orderBy.forEach((item) => {
    order[item.fieldId] = item.direction;
  });
  return order;
}
