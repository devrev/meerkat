import { Graph, checkLoopInGraph, createDirectedGraph } from '../joins/joins';
import { Dimension, JoinEdge, Measure, TableSchema } from '../types/cube-types';

export interface NestedMeasure {
  schema: Measure;
  children: NestedTableSchema[];
}

export interface NestedDimension {
  schema: Dimension;
  children: NestedTableSchema[];
}

export interface NestedTableSchema {
  name: string;
  measures: NestedMeasure[];
  dimensions: NestedDimension[];
}

export const getNestedTableSchema = async (
  tableSchemas: TableSchema[],
  joinPath: JoinEdge[][]
) => {
  const tableSchemaSqlMap: { [key: string]: string } = {};
  for (const schema of tableSchemas) {
    if (!schema) {
      throw new Error('Schema is undefined');
    }
    tableSchemaSqlMap[schema.name] = schema.sql;
  }

  const directedGraph = createDirectedGraph(tableSchemas, tableSchemaSqlMap);
  const hasLoop = checkLoopInGraph(directedGraph);
  if (hasLoop) {
    throw new Error('A loop was detected in the joins.');
  }
  const visitedNodes: { [key: string]: boolean } = {};

  const startingNode = tableSchemas.find(
    (schema) => schema.name === joinPath[0][0].left
  ) as TableSchema;

  visitedNodes[startingNode.name] = true;

  const nestedTableSchema: NestedTableSchema = {
    name: startingNode.name,
    measures: startingNode.measures.map((measure) => ({
      schema: measure,
      children: [],
    })),
    dimensions: startingNode.dimensions.map((dimension) => ({
      schema: dimension,
      children: [],
    })),
  };

  const checkedPaths: { [key: string]: boolean } = {};

  const buildNestedSchema = (
    edges: JoinEdge[],
    index: number,
    nestedTableSchema: NestedTableSchema,
    tableSchemas: TableSchema[]
  ): NestedTableSchema => {
    const edge = edges[index];
    // If the path has been checked before, return the nested schema immediately
    const pathKey = `${edge.left}-${edge.right}-${edge.on}`;
    if (checkedPaths[pathKey]) {
      nestedTableSchema.dimensions.map((dimension) => {
        dimension.children?.forEach((child) => {
          if (child.name === edge.right) {
            return buildNestedSchema(edges, index + 1, child, tableSchemas);
          }
          return;
        });
      });
      return nestedTableSchema;
    }

    const rightSchema = tableSchemas.find(
      (schema) => schema.name === edge.right
    ) as TableSchema;

    // Mark the path as checked
    checkedPaths[pathKey] = true;

    const nestedRightSchema: NestedTableSchema = {
      name: rightSchema.name,
      measures: (rightSchema.measures || []).map(
        (measure) => ({ schema: measure, children: [] } as NestedMeasure)
      ),
      dimensions: (rightSchema.dimensions || []).map(
        (dimension) => ({ schema: dimension, children: [] } as NestedDimension)
      ),
    };

    const nestedDimension = nestedTableSchema.dimensions.find(
      (dimension) => dimension.schema.name === edge.on
    ) as NestedMeasure;

    if (!nestedDimension) {
      throw new Error(
        `The dimension ${edge.on} does not exist in the table schema.`
      );
    }

    nestedDimension.children?.push(nestedRightSchema);

    // Mark the right schema as visited
    visitedNodes[rightSchema.name] = true;

    if (index < edges.length - 1) {
      buildNestedSchema(
        edges,
        index + 1,
        nestedRightSchema as NestedTableSchema,
        tableSchemas
      );
    }
    return nestedTableSchema;
  };

  for (let i = 0; i < joinPath.length; i++) {
    buildNestedSchema(joinPath[i], 0, nestedTableSchema, tableSchemas);
  }

  getNextPossibleNodes(
    directedGraph,
    nestedTableSchema,
    visitedNodes,
    tableSchemas
  );

  return nestedTableSchema;
};

const getNextPossibleNodes = (
  directedGraph: Graph,
  nestedTableSchema: NestedTableSchema,
  visitedNodes: { [key: string]: boolean },
  tableSchemas: TableSchema[]
) => {
  const currentNode = nestedTableSchema.name;

  for (let i = 0; i < nestedTableSchema.dimensions.length; i++) {
    const dimension = nestedTableSchema.dimensions[i];
    const nextPossibleNodes = directedGraph[currentNode];
    if (!nextPossibleNodes || !visitedNodes[currentNode]) {
      return;
    }

    for (const [node] of Object.entries(nextPossibleNodes)) {
      const nextSchema = tableSchemas.find(
        (schema) => schema.name === node
      ) as TableSchema;

      if (!nextSchema) {
        throw new Error(`The schema for ${node} does not exist.`);
      }

      for (const children of dimension.children || []) {
        getNextPossibleNodes(
          directedGraph,
          children,
          visitedNodes,
          tableSchemas
        );
      }

      if (visitedNodes[node]) {
        continue;
      }

      const nestedNextSchema: NestedTableSchema = {
        name: nextSchema.name,
        measures: nextSchema.measures.map((measure) => ({
          schema: measure,
          children: [],
        })),
        dimensions: nextSchema.dimensions.map((dimension) => ({
          schema: dimension,
          children: [],
        })),
      };
      if (
        !dimension.children?.some(
          (child) => child.name === nestedNextSchema.name
        )
      ) {
        dimension.children?.push(nestedNextSchema);
      }
    }
  }
};
