import { DimensionType, MeasureType } from '../types/cube-types';

type CubeType = DimensionType | MeasureType;

export const isArrayTypeMember = (type: CubeType) => {
  return type.endsWith('_array');
};
