import { SelectStatement } from '../types/duckdb-serialization-types';

export interface ParsedSerialization {
  error: string;
  statements: SelectStatement[];
  error_message?: string;
  error_type?: string;
  position?: string;
}
