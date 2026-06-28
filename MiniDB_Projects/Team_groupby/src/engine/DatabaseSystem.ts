import { Tuple, Transaction, StorageProvider } from './types';
import { BufferPoolManager } from './storage/BufferPoolManager';
import { PageManager } from './storage/PageManager';
import { BPlusTree } from './index/BPlusTree';
import { Parser } from './query/Parser';
import { SeqScan, Operator, Filter, NestedLoopJoin, IndexScan } from './query/Operators';
import { CostBasedOptimizer, PlanNode } from './query/Optimizer';
import { TransactionManager } from './tx/TransactionManager';
import { LogManager } from './recovery/LogManager';
import { RecoveryManager } from './recovery/RecoveryManager';

export class DatabaseSystem {
  bufferPool: BufferPoolManager;
  indices: Map<string, BPlusTree> = new Map();
  txManager: TransactionManager = new TransactionManager();
  logManager: LogManager = new LogManager();
  committedTxns: Set<number> = new Set([0]);
  tables: Map<string, { pageId: number; schema: string[] }> = new Map();

  constructor(storageProvider?: StorageProvider) {
    this.bufferPool = new BufferPoolManager(8, storageProvider);
    this.bootstrapData();
  }

  bootstrapData() {
    this.tables.set('users', { pageId: 1, schema: ['id', 'name', 'age'] });
    this.tables.set('orders', { pageId: 2, schema: ['id', 'user_id', 'amount'] });

    this.indices.set('users_pk', new BPlusTree(3));
    this.indices.set('orders_pk', new BPlusTree(3));

    const hasSavedData = this.bufferPool.storageProvider.getItem('minidb_disk_pages') !== null;

    if (hasSavedData) {
      // Load logs and committed transactions first, so we know which txns actually committed
      this.loadLogsAndTxns();

      // Rebuild B+ Tree indices dynamically from loaded Slotted Pages
      for (const [tableName, meta] of this.tables) {
        const page = this.bufferPool.fetchPage(meta.pageId);
        const index = this.indices.get(`${tableName}_pk`);
        if (index) {
          for (let s = 0; s < page.slots.length; s++) {
            const t = PageManager.getTuple(page, s);
            // Rebuild index only for active (visible) and non-deleted/non-uncommitted aborted tuples
            if (t) {
              const isDeletedCommitted = t.xmax !== 0 && this.committedTxns.has(t.xmax);
              if (!isDeletedCommitted) {
                index.insert(t.id, { key: t.id, pageId: meta.pageId, slotId: s });
              }
            }
          }
        }
        this.bufferPool.unpinPage(meta.pageId, false);
      }
    } else {
      // Bootstrap Seed Data
      const userPage = this.bufferPool.fetchPage(1);
      const usersSeed = [
        { id: 10, values: [10, 'Alice', 25] },
        { id: 20, values: [20, 'Bob', 19] },
        { id: 30, values: [30, 'Charlie', 35] }
      ];
      for (const u of usersSeed) {
        const t: Tuple = { id: u.id, values: u.values, xmin: 0, xmax: 0 };
        const slotId = PageManager.insertTuple(userPage, t);
        this.indices.get('users_pk')!.insert(u.id, { key: u.id, pageId: 1, slotId });
      }
      this.bufferPool.unpinPage(1, true);

      const orderPage = this.bufferPool.fetchPage(2);
      const ordersSeed = [
        { id: 101, values: [101, 10, 250] },
        { id: 102, values: [102, 30, 450] }
      ];
      for (const o of ordersSeed) {
        const t: Tuple = { id: o.id, values: o.values, xmin: 0, xmax: 0 };
        const slotId = PageManager.insertTuple(orderPage, t);
        this.indices.get('orders_pk')!.insert(o.id, { key: o.id, pageId: 2, slotId });
      }
      this.bufferPool.unpinPage(2, true);
      this.saveLogsAndTxns();
    }
  }

  loadLogsAndTxns() {
    const savedTxns = this.bufferPool.storageProvider.getItem('minidb_committed_txns');
    if (savedTxns) {
      try {
        const parsed = JSON.parse(savedTxns);
        this.committedTxns = new Set(parsed);
      } catch (e) {
        console.error("Failed to load committed txns", e);
      }
    }

    const savedLogs = this.bufferPool.storageProvider.getItem('minidb_logs');
    if (savedLogs) {
      try {
        const parsed = JSON.parse(savedLogs);
        this.logManager.clearLogs();
        for (const rec of parsed) {
          this.logManager.appendRecord(
            rec.txnId,
            rec.type,
            rec.tableName,
            rec.pageId,
            rec.slotId,
            rec.oldTuple,
            rec.newTuple
          );
        }
      } catch (e) {
        console.error("Failed to load logs from storage provider", e);
      }
    }
  }

  saveLogsAndTxns() {
    const arrTxns = Array.from(this.committedTxns);
    this.bufferPool.storageProvider.setItem('minidb_committed_txns', JSON.stringify(arrTxns));
    const logs = this.logManager.getLogs();
    this.bufferPool.storageProvider.setItem('minidb_logs', JSON.stringify(logs));
  }

  executeSQL(
    sql: string,
    sessionTxn: Transaction | null
  ): {
    results: any[];
    plan?: PlanNode;
    txn?: Transaction | null;
    logsAppended: number;
    error?: string;
  } {
    let logsBefore = this.logManager.getLogs().length;
    let txn = sessionTxn;
    const fetchedPages = new Map<number, boolean>(); // pageId -> isDirty

    const bpFetch = (pageId: number) => {
      const page = this.bufferPool.fetchPage(pageId);
      if (!fetchedPages.has(pageId)) {
        fetchedPages.set(pageId, false);
      }
      return page;
    };

    const bpUnpin = (pageId: number, isDirty: boolean) => {
      this.bufferPool.unpinPage(pageId, isDirty);
      fetchedPages.delete(pageId);
    };

    try {
      const upperSql = sql.trim().toUpperCase();
      if (upperSql === 'RESET' || upperSql === 'RESET DATABASE' || upperSql === 'CLEAR') {
        this.bufferPool.storageProvider.clear();
        this.committedTxns = new Set([0]);
        this.logManager.clearLogs();
        this.bufferPool.clearStorage();
        this.tables.clear();
        this.indices.clear();
        this.bootstrapData();
        return {
          results: ['Database reset successfully to original seed data.'],
          txn: null,
          logsAppended: 0
        };
      }

      if (upperSql === 'CRASH') {
        this.simulateCrash();
        return {
          results: ['System crashed! Buffer pool wiped clean.'],
          txn: null,
          logsAppended: 0
        };
      }

      if (upperSql === 'RECOVER') {
        const res = this.recover();
        return {
          results: [
            `Recovery completed successfully!`,
            `Redone operations: ${res.redoCount}`,
            `Undone operations: ${res.undoCount}`,
            ...res.recoverySteps
          ],
          txn: null,
          logsAppended: 0
        };
      }

      const ast = Parser.parse(sql);

      if (ast.type === 'BEGIN') {
        if (txn) throw new Error("Transaction already active in this session");
        txn = this.txManager.beginTransaction();
        this.logManager.appendRecord(txn.id, 'BEGIN');
        this.saveLogsAndTxns();
        return { results: ['Transaction started'], txn, logsAppended: 1 };
      }

      if (ast.type === 'COMMIT') {
        if (!txn) throw new Error("No active transaction to commit");
        this.txManager.commitTransaction(txn.id);
        this.committedTxns.add(txn.id);
        this.logManager.appendRecord(txn.id, 'COMMIT');
        this.saveLogsAndTxns();
        return { results: ['Transaction committed'], txn: null, logsAppended: 1 };
      }

      if (ast.type === 'ROLLBACK') {
        if (!txn) throw new Error("No active transaction to rollback");
        this.txManager.abortTransaction(txn.id);
        this.logManager.appendRecord(txn.id, 'ABORT');
        this.rollbackTxnActions(txn.id);
        this.saveLogsAndTxns();
        return { results: ['Transaction rolled back'], txn: null, logsAppended: 1 };
      }

      if (ast.type === 'INSERT') {
        const implicitTxn = !txn;
        const activeTxn = txn || this.txManager.beginTransaction();
        const activeTxId = activeTxn.id;

        if (implicitTxn) {
          this.logManager.appendRecord(activeTxId, 'BEGIN');
        }

        const meta = this.tables.get(ast.table);
        if (!meta) throw new Error(`Table ${ast.table} not found`);

        const tupleId = ast.values[0];
        if (typeof tupleId !== 'number') throw new Error("Primary Key must be a number");

        // Primary Key Uniqueness Check
        const index = this.indices.get(`${ast.table}_pk`);
        if (index && index.search(tupleId) !== null) {
          if (implicitTxn) {
            this.txManager.abortTransaction(activeTxId);
          }
          throw new Error(`Constraint Violation: Duplicate primary key ${tupleId} already exists in table ${ast.table}`);
        }

        const page = bpFetch(meta.pageId);

        const tuple: Tuple = {
          id: tupleId,
          values: ast.values,
          xmin: activeTxId,
          xmax: 0
        };

        const slotId = PageManager.insertTuple(page, tuple);
        fetchedPages.set(meta.pageId, true);
        bpUnpin(meta.pageId, true);

        if (index) {
          index.insert(tupleId, { key: tupleId, pageId: meta.pageId, slotId });
        }

        this.logManager.appendRecord(activeTxId, 'INSERT', ast.table, meta.pageId, slotId, undefined, tuple);

        if (implicitTxn) {
          this.txManager.commitTransaction(activeTxId);
          this.committedTxns.add(activeTxId);
          this.logManager.appendRecord(activeTxId, 'COMMIT');
        }

        this.saveLogsAndTxns();

        return {
          results: [`Inserted 1 record (ID: ${tupleId})`],
          txn,
          logsAppended: this.logManager.getLogs().length - logsBefore
        };
      }

      if (ast.type === 'DELETE') {
        const implicitTxn = !txn;
        const activeTxn = txn || this.txManager.beginTransaction();
        const activeTxId = activeTxn.id;

        if (implicitTxn) {
          this.logManager.appendRecord(activeTxId, 'BEGIN');
        }

        const meta = this.tables.get(ast.table);
        if (!meta) throw new Error(`Table ${ast.table} not found`);

        const page = bpFetch(meta.pageId);
        let deletedCount = 0;

        for (let s = 0; s < page.slots.length; s++) {
          const rawT = PageManager.getTuple(page, s);
          if (rawT) {
            const isVisible = txn
              ? TransactionManager.isTupleVisible(rawT, txn.id, txn.snapshotActiveTxns, this.committedTxns)
              : TransactionManager.isTupleVisible(rawT, Infinity, [], this.committedTxns);

            if (isVisible) {
              if (!ast.where || this.evalCondition(rawT, ast.where, meta.schema)) {
                // Write-Write Conflict Check
                if (rawT.xmax !== 0 && rawT.xmax !== activeTxId) {
                  if (implicitTxn) {
                    this.txManager.abortTransaction(activeTxId);
                  }
                  throw new Error(`Serialization Failure: Concurrent update/delete conflict on key ${rawT.id} (locked by transaction ${rawT.xmax})`);
                }

                const oldTuple = { ...rawT };
                rawT.xmax = activeTxId;
                PageManager.updateTuple(page, s, rawT);
                deletedCount++;
                
                // Keep B+ Tree index in sync
                const index = this.indices.get(`${ast.table}_pk`);
                if (index) {
                  index.delete(rawT.id);
                }

                this.logManager.appendRecord(activeTxId, 'DELETE', ast.table, meta.pageId, s, oldTuple, rawT);
              }
            }
          }
        }
        fetchedPages.set(meta.pageId, true);
        bpUnpin(meta.pageId, true);

        if (implicitTxn) {
          this.txManager.commitTransaction(activeTxId);
          this.committedTxns.add(activeTxId);
          this.logManager.appendRecord(activeTxId, 'COMMIT');
        }

        this.saveLogsAndTxns();

        return {
          results: [`Deleted ${deletedCount} records`],
          txn,
          logsAppended: this.logManager.getLogs().length - logsBefore
        };
      }

      if (ast.type === 'SELECT') {
        const meta = this.tables.get(ast.table);
        if (!meta) throw new Error(`Table ${ast.table} not found`);

        const page = bpFetch(meta.pageId);
        
        const visibleTuples: Tuple[] = [];
        for (let s = 0; s < page.slots.length; s++) {
          const t = PageManager.getTuple(page, s);
          if (t) {
            const isVisible = txn
              ? TransactionManager.isTupleVisible(t, txn.id, txn.snapshotActiveTxns, this.committedTxns)
              : TransactionManager.isTupleVisible(t, Infinity, [], this.committedTxns);
            if (isVisible) visibleTuples.push(t);
          }
        }
        bpUnpin(meta.pageId, false);

        let joinTuples: Tuple[] = [];
        let joinMeta = ast.joinTable ? this.tables.get(ast.joinTable) : null;
        if (joinMeta) {
          const joinPage = bpFetch(joinMeta.pageId);
          for (let s = 0; s < joinPage.slots.length; s++) {
            const t = PageManager.getTuple(joinPage, s);
            if (t) {
              const isVisible = txn
                ? TransactionManager.isTupleVisible(t, txn.id, txn.snapshotActiveTxns, this.committedTxns)
                : TransactionManager.isTupleVisible(t, Infinity, [], this.committedTxns);
              if (isVisible) joinTuples.push(t);
            }
          }
          bpUnpin(joinMeta.pageId, false);
        }

        const hasIndex = this.indices.has(`${ast.table}_pk`);
        const indexKeyPresent = ast.where ? ast.where.column === 'id' : false;
        const plan = CostBasedOptimizer.selectBestPlan(ast, visibleTuples.length, hasIndex, indexKeyPresent);

        // Real Index Scan Execution using direct B+ Tree coordinates
        let scanOp: Operator = new SeqScan(visibleTuples);
        if (plan.type === 'IndexScan' && ast.where) {
          const index = this.indices.get(`${ast.table}_pk`)!;
          const lookup = index.search(ast.where.value);
          if (lookup) {
            scanOp = new IndexScan(this.bufferPool, lookup.pageId, lookup.slotId, t => {
              return txn
                ? TransactionManager.isTupleVisible(t, txn.id, txn.snapshotActiveTxns, this.committedTxns)
                : TransactionManager.isTupleVisible(t, Infinity, [], this.committedTxns);
            });
          } else {
            scanOp = new IndexScan(this.bufferPool, -1, -1);
          }
        }

        let execOp: Operator = scanOp;
        if (ast.where && plan.type !== 'IndexScan') {
          const cond = ast.where;
          execOp = new Filter(scanOp, t => this.evalCondition(t, cond, meta.schema));
        }

        if (joinMeta && ast.joinOn) {
          const joinCond = ast.joinOn;
          const leftIdx = meta.schema.indexOf(joinCond.left.split('.')[1] || joinCond.left);
          const rightIdx = joinMeta.schema.indexOf(joinCond.right.split('.')[1] || joinCond.right);
          const innerOp = new SeqScan(joinTuples);
          execOp = new NestedLoopJoin(execOp, innerOp, (o, i) => o.values[leftIdx] === i.values[rightIdx]);
        }

        execOp.init();
        const output: Tuple[] = [];
        let item: Tuple | null;
        while ((item = execOp.next()) !== null) {
          output.push(item);
        }
        execOp.close();

        const finalSchema = joinMeta ? [...meta.schema, ...joinMeta.schema] : meta.schema;
        const selectColumns = (ast.columns.length === 1 && ast.columns[0] === '*')
          ? finalSchema
          : ast.columns;

        const results = output.map(tuple => {
          const formatted: any = {};
          selectColumns.forEach(col => {
            const rawColName = col.includes('.') ? col.split('.')[1] : col;
            const fullColName = col.includes('.') ? col : col;
            let idx = finalSchema.indexOf(rawColName);
            if (idx === -1) idx = finalSchema.indexOf(col);
            if (idx !== -1) {
              formatted[fullColName] = tuple.values[idx];
            }
          });
          return formatted;
        });

        return {
          results,
          plan,
          txn,
          logsAppended: 0
        };
      }
    } catch (err: any) {
      return {
        results: [],
        txn,
        logsAppended: 0,
        error: err.message
      };
    } finally {
      // Centralized automatic clean up of pinned frames on query errors
      for (const [pageId, isDirty] of fetchedPages) {
        this.bufferPool.unpinPage(pageId, isDirty);
      }
    }
    return { results: [], txn, logsAppended: 0 };
  }

  private evalCondition(tuple: Tuple, where: { column: string; op: string; value: any }, schema: string[]): boolean {
    const idx = schema.indexOf(where.column);
    if (idx === -1) return false;
    const val = tuple.values[idx];
    if (where.op === '=') return val === where.value;
    if (where.op === '>') return val > where.value;
    if (where.op === '<') return val < where.value;
    return false;
  }

  private rollbackTxnActions(txnId: number) {
    for (const [tableName, meta] of this.tables) {
      const page = this.bufferPool.fetchPage(meta.pageId);
      for (let s = 0; s < page.slots.length; s++) {
        const tuple = PageManager.getTuple(page, s);
        if (tuple) {
          if (tuple.xmin === txnId) {
            PageManager.deleteTuple(page, s);
            const index = this.indices.get(`${tableName}_pk`);
            if (index) {
              index.delete(tuple.id);
            }
          }
          if (tuple.xmax === txnId) {
            tuple.xmax = 0;
            PageManager.updateTuple(page, s, tuple);
            const index = this.indices.get(`${tableName}_pk`);
            if (index) {
              index.insert(tuple.id, { key: tuple.id, pageId: meta.pageId, slotId: s });
            }
          }
        }
      }
      this.bufferPool.unpinPage(meta.pageId, true);
    }
  }

  simulateCrash(): number {
    const logCount = this.logManager.getLogs().length;
    const provider = this.bufferPool.storageProvider;
    this.bufferPool = new BufferPoolManager(8, provider);
    this.committedTxns = new Set([0]);

    // Rebuild B+ Tree indices dynamically from the post-crash slotted pages
    for (const [tableName, meta] of this.tables) {
      this.indices.set(`${tableName}_pk`, new BPlusTree(3));
      const page = this.bufferPool.fetchPage(meta.pageId);
      const index = this.indices.get(`${tableName}_pk`);
      if (index) {
        for (let s = 0; s < page.slots.length; s++) {
          const t = PageManager.getTuple(page, s);
          if (t) {
            const isDeletedCommitted = t.xmax !== 0 && this.committedTxns.has(t.xmax);
            if (!isDeletedCommitted) {
              index.insert(t.id, { key: t.id, pageId: meta.pageId, slotId: s });
            }
          }
        }
      }
      this.bufferPool.unpinPage(meta.pageId, false);
    }

    return logCount;
  }

  recover(): { redoCount: number; undoCount: number; recoverySteps: string[] } {
    const logs = this.logManager.getLogs();
    const result = RecoveryManager.performARIESRecovery(logs, this.bufferPool, this.committedTxns, this.indices);
    this.saveLogsAndTxns();
    return result;
  }
}
