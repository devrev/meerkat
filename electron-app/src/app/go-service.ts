import { ChildProcess, spawn } from 'child_process';
import * as path from 'path';

export class GoService {
  private process: ChildProcess | null = null;
  private ready = false;
  private commandQueue: {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
  }[] = [];

  public async start(): Promise<boolean> {
    if (this.process) return this.ready;

    return new Promise((resolve) => {
      // Use the correct path to your Go executable
      const execPath = path.join(
        process.cwd(),
        'electron-app/src/app/duckdb_processor'
      );

      try {
        this.process = spawn(execPath, [], {
          cwd: path.dirname(execPath),
        });

        this.process.stdout.on('data', (data) => {
          const output = data.toString().trim();
          console.log(`Go service output: "${output}"`);

          if (output === 'READY') {
            console.log('Go service is ready!');
            this.ready = true;
            resolve(true);
          } else if (output.startsWith('ERROR: ')) {
            const error = output.substring(7);
            console.error('Go service error:', error);
            const cmd = this.commandQueue.shift();

            if (cmd) cmd.reject(new Error(error));
          } else {
            // Try to parse as JSON (direct result without prefix)
            try {
              const data = JSON.parse(output);

              const cmd = this.commandQueue.shift();
              if (cmd) cmd.resolve(data);
            } catch (err) {
              console.error('Error parsing output as JSON:', err);
              console.error('Raw output:', output);
            }
          }
        });

        this.process.stderr.on('data', (data) => {
          console.error(`Go service stderr: ${data.toString().trim()}`);
        });

        this.process.on('error', (err) => {
          console.error(`Failed to start Go service: ${err}`);
          resolve(false);
        });

        this.process.on('close', (code) => {
          console.log(`Go service process exited with code ${code}`);
          this.ready = false;
          this.process = null;
        });
      } catch (error) {
        console.error('Error starting Go service:', error);
        resolve(false);
      }
    });
  }

  public async query(sql: string): Promise<any[]> {
    if (!this.ready) {
      const started = await this.start();
      if (!started) {
        throw new Error('Failed to start Go service');
      }
    }

    return new Promise((resolve, reject) => {
      this.commandQueue.push({ resolve, reject });

      console.log(`Sending query to Go service: ${sql}`);
      this.process.stdin.write(`QUERY:${sql}\n`);
    });
  }
}

const goService = new GoService();

export default goService;
