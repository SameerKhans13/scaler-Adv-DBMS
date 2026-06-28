import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { StorageProvider } from './engine/types';
import { DatabaseSystem } from './engine/DatabaseSystem';

export class FileStorageProvider implements StorageProvider {
  private dataDir: string;

  constructor() {
    this.dataDir = path.resolve(process.cwd(), '.minidb_data');
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  private getFilePath(key: string): string {
    return path.join(this.dataDir, `${key}.json`);
  }

  getItem(key: string): string | null {
    const file = this.getFilePath(key);
    if (fs.existsSync(file)) {
      try {
        return fs.readFileSync(file, 'utf8');
      } catch (e) {
        console.error(`Failed to read file ${file}`, e);
      }
    }
    return null;
  }

  setItem(key: string, value: string): void {
    const file = this.getFilePath(key);
    try {
      fs.writeFileSync(file, value, 'utf8');
    } catch (e) {
      console.error(`Failed to write file ${file}`, e);
    }
  }

  removeItem(key: string): void {
    const file = this.getFilePath(key);
    if (fs.existsSync(file)) {
      try {
        fs.unlinkSync(file);
      } catch (e) {
        console.error(`Failed to delete file ${file}`, e);
      }
    }
  }

  clear(): void {
    if (fs.existsSync(this.dataDir)) {
      try {
        const files = fs.readdirSync(this.dataDir);
        for (const file of files) {
          fs.unlinkSync(path.join(this.dataDir, file));
        }
      } catch (e) {
        console.error(`Failed to clear data directory`, e);
      }
    }
  }
}

// Instantiate the system with the physical file storage provider
const db = new DatabaseSystem(new FileStorageProvider());

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

console.log('\x1b[35m====================================================\x1b[0m');
console.log('\x1b[1m\x1b[36m            MINIDB INTERACTIVE SQL CLIENT            \x1b[0m\x1b[22m');
console.log('\x1b[35m====================================================\x1b[0m');
console.log(`\x1b[90mData storage path:\x1b[0m \x1b[4m${path.resolve(process.cwd(), '.minidb_data')}\x1b[24m`);
console.log('Enter your SQL queries below. Type \x1b[33m"exit"\x1b[0m or \x1b[33m"quit"\x1b[0m to leave.\n');

let currentTxn: any = null;

function prompt() {
  const prefix = currentTxn ? `\x1b[32mminidb-cli (Txn ${currentTxn.id})>\x1b[0m ` : '\x1b[34mminidb-cli>\x1b[0m ';
  rl.question(prefix, (input: string) => {
    const trimmed = input.trim();
    if (trimmed.toLowerCase() === 'exit' || trimmed.toLowerCase() === 'quit') {
      rl.close();
      return;
    }

    if (trimmed === '') {
      prompt();
      return;
    }

    try {
      const res = db.executeSQL(trimmed, currentTxn);
      if (res.error) {
        console.log(`\x1b[31mError: ${res.error}\x1b[0m`);
      } else {
        currentTxn = res.txn;
        console.log('\x1b[32mExecution Results:\x1b[0m');
        if (Array.isArray(res.results)) {
          if (res.results.length === 0) {
            console.log('No records found / Empty set');
          } else {
            console.table(res.results);
          }
        } else {
          console.log(res.results);
        }
        if (res.plan) {
          console.log('\x1b[36mQuery Plan:\x1b[0m', JSON.stringify(res.plan, null, 2));
        }
        if (res.logsAppended > 0) {
          console.log(`\x1b[90m(Logs appended: ${res.logsAppended})\x1b[0m`);
        }
      }
    } catch (e: any) {
      console.log(`\x1b[31mUnexpected Error: ${e.message}\x1b[0m`);
    }
    console.log();
    prompt();
  });
}

prompt();
