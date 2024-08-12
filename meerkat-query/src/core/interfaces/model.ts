import { QueryBuilder } from '../../query/interfaces/query';
import { Table } from './table';

export interface Model extends Table {
  query: QueryBuilder;
  lastMaterialized: Date | null;

  materialize(): Promise<boolean>;
  updateQuery(newQuery: QueryBuilder): void;
  getLastMaterializedDate(): Date | null;
  isStale(threshold: number): boolean;
  refreshData(): Promise<boolean>;
  getColumnLineage(fieldName: string): string[];
  getDependentModels(): Model[];
}
