import path = require('path');
import { Graph, checkLoopInGraph, createDirectedGraph } from '../joins/joins';
import { Dimension, JoinEdge, Measure, TableSchema } from '../types/cube-types';

export interface NestedMeasure extends Measure {
  children?: NestedTableSchema[];
}

export interface NestedDimension extends Dimension {
  children?: NestedTableSchema[];
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
  for (let schema of tableSchemas) {
    console.info(tableSchemaSqlMap);
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
      ...measure,
      children: [],
    })),
    dimensions: startingNode.dimensions.map((dimension) => ({
      ...dimension,
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
        });
      });
      return nestedTableSchema;
    }

    const rightSchema = tableSchemas.find(
      (schema) => schema.name === edge.right
    ) as TableSchema;

    // Mark the path as checked
    checkedPaths[pathKey] = true;

    let nestedRightSchema: NestedTableSchema = {
      name: rightSchema.name,
      measures: (rightSchema.measures || []).map(
        (measure) => ({ ...measure, children: [] } as NestedMeasure)
      ),
      dimensions: (rightSchema.dimensions || []).map(
        (dimension) => ({ ...dimension, children: [] } as NestedDimension)
      ),
    };

    const nestedDimension = nestedTableSchema.dimensions.find(
      (dimension) => dimension.name === edge.on
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
    console.info(nestedTableSchema);
    buildNestedSchema(joinPath[i], 0, nestedTableSchema, tableSchemas);
  }

  getNextPossibleNodes(
    directedGraph,
    nestedTableSchema,
    visitedNodes,
    tableSchemas
  );

  getNextPossibleNodes(
    directedGraph,
    nestedTableSchema,
    visitedNodes,
    tableSchemas
  );

  console.log(JSON.stringify(nestedTableSchema, null, 2));

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
    const dimensionName = dimension.name;
    const nextPossibleNodes = directedGraph[currentNode];

    if (!nextPossibleNodes) {
      return;
    }

    for (let [node, edge_on] of Object.entries(nextPossibleNodes)) {
      if (visitedNodes[node]) {
        continue;
      }
      const nextSchema = tableSchemas.find(
        (schema) => schema.name === node
      ) as TableSchema;

      if (!nextSchema) {
        throw new Error(`The schema for ${node} does not exist.`);
      }

      for (let children of dimension.children || []) {
        getNextPossibleNodes(
          directedGraph,
          children,
          visitedNodes,
          tableSchemas
        );
      }
      for (let key in edge_on) {
        const nestedNextSchema: NestedTableSchema = {
          name: nextSchema.name,
          measures: nextSchema.measures.map((measure) => ({
            ...measure,
            children: [],
          })),
          dimensions: nextSchema.dimensions.map((dimension) => ({
            ...dimension,
            children: [],
          })),
        };

        dimension.children?.push(nestedNextSchema);
      }
    }
  }
};
