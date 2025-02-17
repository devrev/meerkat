import { DimensionType } from '../types/cube-types';

export interface Column<T = object> {
  name: string;
  dataType: DimensionType;
  schema?: T;
}

export interface Dataset {
  id: string;
  name: string;
  columns: Column[];
}
