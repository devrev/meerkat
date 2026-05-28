import { Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';

/** Successful decomposition: the SQL was split into a reusable schema + query. */
export interface DecomposeResult {
  success: true;
  tableSchema: TableSchema;
  query: Query;
  warnings: string[];
}

/** Failed decomposition: the SQL structure is unsupported or unparseable. */
export interface DecomposeFailure {
  success: false;
  reason: string;
}

export type DecomposeOutput = DecomposeResult | DecomposeFailure;
