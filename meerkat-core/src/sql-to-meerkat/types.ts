import { Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';

export interface DecomposeResult {
  success: true;
  tableSchema: TableSchema;
  query: Query;
  warnings: string[];
}

export interface DecomposeFailure {
  success: false;
  reason: string;
}

export type DecomposeOutput = DecomposeResult | DecomposeFailure;

export interface ExtractedFilter {
  member: string;
  operator: string;
  values: string[];
}
