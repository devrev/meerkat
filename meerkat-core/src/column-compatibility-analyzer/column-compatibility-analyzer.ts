import isEqual from 'lodash/isEqual';
import { DatasetRegistry } from '../dataset-registry/dataset-registry';
import { Column } from '../dataset-registry/types';
import { DimensionType } from '../types/cube-types';
import {
  NAME_EXACT_MATCH,
  NAME_PARTIAL_MATCH,
  SCHEMA_COMPATIBILITY_MATCH,
  TYPE_COMPATIBILITY_MATCH,
} from './constants';
import { CompatibleColumn, CompatibleColumnsRequest } from './types';

export class ColumnCompatibilityAnalyzer {
  constructor(private readonly registry: DatasetRegistry) {}

  public findCompatibleColumns({
    sourceDatasetId,
    sourceColumnName,
  }: CompatibleColumnsRequest): CompatibleColumn[] {
    const sourceColumn = this.registry.findColumnInDataset({
      datasetId: sourceDatasetId,
      columnName: sourceColumnName,
    });

    return this.findMatchingColumns({
      sourceColumn,
      datasetId: sourceDatasetId,
    });
  }

  private findMatchingColumns({
    sourceColumn,
    datasetId,
  }: {
    sourceColumn: Column;
    datasetId: string;
  }): CompatibleColumn[] {
    const datasets = this.registry.getDatasets();

    const compatibleColumns = datasets
      .filter((dataset) => dataset.id !== datasetId)
      .flatMap((dataset) =>
        dataset.columns
          .map((column) => ({
            dataset,
            column,
            score: this.assessCompatibilityScore(sourceColumn, column),
          }))
          .filter((result) => result.score > 0)
      );

    return compatibleColumns
      .sort((a, b) => b.score - a.score)
      .map(({ dataset, column }) => ({ dataset, column }));
  }

  private assessCompatibilityScore(
    sourceColumnName: Column,
    targetColumn: Column
  ): number {
    const typeScore = this.getTypeCompatibilityScore(
      sourceColumnName.dataType,
      targetColumn.dataType
    );

    if (typeScore === 0) {
      return 0;
    }

    const nameScore = this.getNameSimilarityScore(
      sourceColumnName.name,
      targetColumn.name
    );
    const schemaScore = this.getSchemaCompatibilityScore(
      sourceColumnName,
      targetColumn
    );

    return typeScore + nameScore + schemaScore;
  }

  private getTypeCompatibilityScore(
    sourceType: DimensionType,
    targetType: DimensionType
  ): number {
    return sourceType === targetType ? TYPE_COMPATIBILITY_MATCH : 0;
  }

  private getNameSimilarityScore(
    sourceName: string,
    targetName: string
  ): number {
    const sourceNormalized = this.normalizeColumnName(sourceName);
    const targetNormalized = this.normalizeColumnName(targetName);

    if (sourceNormalized === targetNormalized) {
      return NAME_EXACT_MATCH;
    }

    if (
      sourceNormalized.includes(targetNormalized) ||
      targetNormalized.includes(sourceNormalized)
    ) {
      return NAME_PARTIAL_MATCH;
    }

    return 0;
  }

  private getSchemaCompatibilityScore(
    sourceColumnName: Column,
    targetColumn: Column
  ): number {
    if (!sourceColumnName.schema || !targetColumn.schema) {
      return 0;
    }

    return isEqual(sourceColumnName.schema, targetColumn.schema)
      ? SCHEMA_COMPATIBILITY_MATCH
      : 0;
  }

  private normalizeColumnName(name: string): string {
    return name.toLowerCase().replace(/[_-]/g, '');
  }
}
