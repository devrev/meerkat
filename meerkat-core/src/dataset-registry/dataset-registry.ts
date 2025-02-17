import { Column, Dataset } from './types';

export class DatasetRegistry {
  private datasets: Map<string, Dataset> = new Map();

  registerDataset(dataset: Dataset): void {
    this.datasets.set(dataset.id, dataset);
  }

  getDataset(id: string): Dataset | undefined {
    return this.datasets.get(id);
  }

  getDatasets(): Dataset[] {
    return Array.from(this.datasets.values());
  }

  deleteDataset(id: string): void {
    this.datasets.delete(id);
  }

  clear(): void {
    this.datasets.clear();
  }

  findColumnInDataset({
    datasetId,
    columnName,
  }: {
    datasetId: string;
    columnName: string;
  }): Column {
    const dataset = this.getDataset(datasetId);

    if (!dataset) {
      throw new Error(`Dataset ${datasetId} not found`);
    }

    const column = dataset.columns.find((col) => col.name === columnName);

    if (!column) {
      throw new Error(
        `Column ${columnName} not found in dataset ${dataset.id}`
      );
    }

    return column;
  }
}
