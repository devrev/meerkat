import { Column, Dataset } from '../dataset-registry/types';

export interface CompatibleColumnsRequest {
  sourceDatasetId: string;
  sourceColumnName: string;
}

export interface CompatibilityScore {
  score: number;
}

export interface CompatibleColumn {
  dataset: Dataset;
  column: Column;
}

export interface JoinPath {
  sourceDatasetId: string;
  sourceColumnName: string;
  destinationDatasetId: string;
  destinationColumnName: string;
}
