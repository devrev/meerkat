// worker-manager.js
export class WorkerManager {
  worker: Worker;

  constructor(onMessage: any) {
    this.worker = new Worker('duckdb-worker.js');
    this.worker.onmessage = onMessage;
  }

  sendMessage(message: any) {
    this.worker.postMessage(message);
  }

  terminate() {
    this.worker.terminate();
  }
}
