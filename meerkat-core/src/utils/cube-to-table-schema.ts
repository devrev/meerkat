import { DimensionType, Measure, TableSchema } from '../types/cube-types/table';

export type CubeMeasureType =
  | 'string'
  | 'string_array'
  | 'time'
  | 'number'
  | 'number_array'
  | 'boolean'
  | 'count'
  | 'count_distinct'
  | 'count_distinct_approx'
  | 'sum'
  | 'avg'
  | 'min'
  | 'max';

interface CubeSchema {
  name: string;
  object: {
    sql_table: string;
    measures: {
      [key: string]: CubeMeasure;
    };
    dimensions: {
      [key: string]: {
        sql: string;
        type: DimensionType;
      };
    };
    joins?: {
      [key: string]: {
        sql: string;
      };
    };
  };
}
const cube = (name: any, object: any) => ({ name, object });
export function convertCubeStringToTableSchema(
  file: string
): TableSchema | null {
  // replace ${...} with ...
  let replacedFile = file.replace(/\${(.*?)}/g, (match, variable) => variable);

  // replace CUBE with MEERKAT
  replacedFile = replacedFile.replace(/CUBE/g, '{MEERKAT}');
  const { name, object } = eval(replacedFile);
  return convertCubeToTableSchema({ name, object });
}

function convertCubeToTableSchema({
  name,
  object,
}: CubeSchema): TableSchema | null {
  const resObj: TableSchema = {
    name,
    sql: object.sql_table,
    measures: [],
    dimensions: [],
  };
  const dimensions = object.dimensions;
  const measures = object.measures;

  // convert dimensions and measures
  for (const key in dimensions) {
    resObj.dimensions.push({
      name: key,
      sql: dimensions[key].sql,
      type: dimensions[key].type,
    });
  }
  for (const key in measures) {
    resObj.measures.push({ name: key, ...convertMeasure(measures[key]) });
  }

  // convert joins
  if (object.joins && Object.keys(object.joins).length > 0) {
    resObj.joins = [];
    for (const joinName in object.joins) {
      const join = object.joins[joinName];
      resObj.joins.push({
        sql: join.sql,
      });
    }
  }
  return resObj;
}
interface CubeMeasure {
  sql?: string;
  type: CubeMeasureType;
}

function convertMeasure(measure: CubeMeasure): Pick<Measure, 'sql' | 'type'> {
  switch (measure.type) {
    case 'count':
      return {
        sql: measure.sql ? `COUNT(${measure.sql})` : 'COUNT(*)',
        type: 'number',
      };
    case 'count_distinct':
      return {
        sql: `COUNT(DISTINCT ${measure.sql})`,
        type: 'number',
      };
    case 'count_distinct_approx':
      return {
        sql: `APPROX_COUNT_DISTINCT(${measure.sql})`,
        type: 'number',
      };
    case 'sum':
      return {
        sql: `SUM(${measure.sql})`,
        type: 'number',
      };
    case 'avg':
      return {
        sql: `AVG(${measure.sql})`,
        type: 'number',
      };
    case 'min':
      return {
        sql: `MIN(${measure.sql})`,
        type: 'number',
      };
    case 'max':
      return {
        sql: `MAX(${measure.sql})`,
        type: 'number',
      };
    default:
      return {
        sql: measure.sql ?? '',
        type: measure.type,
      };
  }
}
