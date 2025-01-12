import { SelectStatement } from '../types/duckdb-serialization-types';

export interface ParsedSerialization {
  statements: SelectStatement[];
  error?: boolean;
  error_message?: string;
  error_type?: string;
  position?: string;
}
