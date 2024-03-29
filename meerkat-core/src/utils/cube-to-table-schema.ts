import { TableSchema } from '../types/cube-types/table';

export function convertCubeToTableSchema(file: string): TableSchema | null {
  const cube = (name: any, object: any) => ({ name, object });
  let replacedFile = file.replace(/\${(.*?)}/g, (match, variable) => variable);
  replacedFile = replacedFile.replace(/CUBE/g, 'MEERKAT');
  const { name, object } = eval(replacedFile);
  const resObj: TableSchema = {
    name,
    sql: object.sql_table,
    measures: [],
    dimensions: [],
  };
  const dimensions = object.dimensions;
  const measures = object.measures;
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

function convertMeasure(measure: any): any {
  switch (measure.type) {
    case 'count':
      return {
        sql: measure.sql ? 'COUNT(' + measure.sql + ')' : 'COUNT(*)',
        type: 'number',
      };
    case 'count_distinct':
      return {
        sql: 'COUNT(DISTINCT ' + measure.sql + ')',
        type: 'number',
      };
    case 'count_distinct_approx':
      return {
        sql: 'COUNT(DISTINCT ' + measure.sql + ')',
        type: 'number',
      };
    case 'sum':
      return {
        sql: 'SUM(' + measure.sql + ')',
        type: 'number',
      };
    case 'avg':
      return {
        sql: 'AVG(' + measure.sql + ')',
        type: 'number',
      };
    case 'min':
      return {
        sql: 'MIN(' + measure.sql + ')',
        type: 'number',
      };
    case 'max':
      return {
        sql: 'MAX(' + measure.sql + ')',
        type: 'number',
      };
    default:
      return {
        sql: measure.sql,
        type: measure.type,
      };
  }
}
