import { DimensionType, MeasureType } from '../types/cube-types';

type CubeType = DimensionType | MeasureType;

export const CUBE_TYPE_TO_DUCKDB_TYPE: {
  [key in CubeType]: string;
} = {
  string: 'VARCHAR',
  number: 'DECIMAL',
  time: 'VARCHAR',
  boolean: 'BOOLEAN',
};
