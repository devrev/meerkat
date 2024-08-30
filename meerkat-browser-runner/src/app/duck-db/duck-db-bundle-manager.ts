import { DuckDBBundle, DuckDBBundles, selectBundle } from '@devrev/duckdb-wasm';
import duckdb_wasm_eh from '@devrev/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdb_wasm from '@devrev/duckdb-wasm/dist/duckdb-mvp.wasm?url';

class DuckDbBundleManager {
  private inputBundles: DuckDBBundles;
  private bundles: DuckDBBundle | undefined;
  private static instance: DuckDbBundleManager;
  private bundleResolverPromise: Promise<DuckDBBundle> | undefined;

  constructor(inputBundles: DuckDBBundles) {
    if (DuckDbBundleManager.instance) {
      throw new Error('DuckDbBundleProvider instance already created');
    }
    this.inputBundles = inputBundles;
    DuckDbBundleManager.instance = this;
  }

  private createBundleResolverPromise(): Promise<DuckDBBundle> {
    return new Promise((resolve, reject) => {
      selectBundle(this.inputBundles)
        .then((bundle) => {
          this.bundles = bundle;
          resolve(bundle);
        })
        .catch((error) => reject(error));
    });
  }

  public async resolveBundle() {
    if (!this.bundleResolverPromise) {
      this.bundleResolverPromise = this.createBundleResolverPromise();
    }
    return await this.bundleResolverPromise;
  }
}

export const DuckDbBundleManagerInstance = new DuckDbBundleManager({
  eh: {
    mainModule: duckdb_wasm_eh,
    mainWorker: new URL(
      '@devrev/duckdb-wasm/dist/duckdb-browser-eh.worker.js',
      import.meta.url
    ).toString(),
  },
  mvp: {
    mainModule: duckdb_wasm,
    mainWorker: new URL(
      '@devrev/duckdb-wasm/dist/duckdb-browser-mvp.worker.js',
      import.meta.url
    ).toString(),
  },
});
