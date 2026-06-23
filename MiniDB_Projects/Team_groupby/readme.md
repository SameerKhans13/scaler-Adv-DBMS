# MiniDB Relational Engine — Team groupby

A high-performance relational database engine playground implementing advanced DBMS internals: slotted-page storage, B+ Tree indexing, Volcano-style physical execution, cost-based optimization, PostgreSQL-style Multi-Version Concurrency Control (MVCC), and ARIES crash recovery. It features a state-of-the-art interactive web dashboard for real-time visualization.

## Team members
- **Sameer Khan** (sam13sameer.13khan@gmail.com)
- **Rishi Harti** (rishie.harti@gmail.com)
- **Srujan Gowda** (kssrujangowda37@gmail.com)
- **Pranay Reddy** (pranay.24bcs10133@sst.scaler.com)

---

## 1. Problem Statement
Relational databases are the backbone of modern software architecture, yet understanding their low-level mechanics (buffer paging, version visibility, index page splits, and recovery replay) is challenging due to the complexity of production engines. 

**MiniDB** addresses this by providing a clean, modular, and fully visible database engine written in TypeScript. It simulates a relational database from the byte level up, exposing internal components (such as buffer pool frames, disk pages, B+ Tree node structures, WAL records, and execution plans) inside an interactive visual environment.

---

## 2. Architecture Diagram

```
                 +---------------------------------------+
                 |            SQL Query Text             |
                 +---------------------------------------+
                                     |
                                     v
                       +---------------------------+
                       |          Parser           |
                       +---------------------------+
                                     |  (Abstract Syntax Tree)
                                     v
                       +---------------------------+
                       |   Cost-Based Optimizer    |
                       +---------------------------+
                                     |  (Best Physical Plan)
                                     v
                       +---------------------------+
                       |    Volcano Executor       |
                       | (SeqScan, IndexScan, Join)|
                       +---------------------------+
                          |                     |
  (Read/Write Requests)   |                     | (Snapshot Visibility check)
                          v                     v
+-----------------------------+       +-----------------------------+
|     Buffer Pool Manager     |       |     Transaction Manager     |
+-----------------------------+       +-----------------------------+
| - Cache frames (pin/unpin)  |       | - MVCC Isolation            |
| - LRA Eviction Policy       |       | - Active snapshot tracking  |
+-----------------------------+       +-----------------------------+
               |                                     |
               v (Slotted Page formatting)           v (Write-Ahead Logging)
+-----------------------------+       +-----------------------------+
|        Page Manager         | <---> |        Log Manager          |
|  - Tracks slots, offsets    |       |  - Append WAL Records       |
+-----------------------------+       +-----------------------------+
               |                                     |
               v (Simulated Disk)                    v (ARIES 3-Phase Replay)
+-----------------------------+       +-----------------------------+
|        Disk Storage         | <---> |      Recovery Manager       |
+-----------------------------+       +-----------------------------+
```

---

## 3. Major Modules

1. **Storage Engine (`PageManager` & `BufferPoolManager`)**: Manages physical layout and frame caches. Translates logical pages to disk-equivalent memory buffers.
2. **Indexing Module (`BPlusTree`)**: Dynamically indexes records by primary key to enable fast search, insert, and deletion with \(O(\log N)\) complexity.
3. **Query Engine (`Parser` & `Operators`)**: Parses subset SQL into Abstract Syntax Trees (ASTs) and executes queries using Volcano physical operators.
4. **Optimization Module (`CostBasedOptimizer`)**: Performs cost estimates to select either a full table Scan (`SeqScan`) or B+ Tree search (`IndexScan`).
5. **Concurrency Manager (`TransactionManager`)**: Runs snapshot isolation transactions via version visibility metadata checking.
6. **Recovery Engine (`RecoveryManager` & `LogManager`)**: Records WAL log operations and executes ARIES recovery phases after system crashes.

---

## 4. Data Flow

When a query is run, the engine processes data as follows:
1. **Parsing**: The `Parser` tokenizes the SQL string and outputs an AST representation.
2. **Optimization**: The `CostBasedOptimizer` receives the AST, estimates query selectivity, evaluates table statistics, calculates scan costs, and outputs the optimal `PlanNode`.
3. **Transaction Session Setup**: The session's active transaction context provides a transaction ID and a snapshot of concurrent active transactions (`snapshotActiveTxns`).
4. **Execution Plan Construction**: The Volcano operators (e.g., `SeqScan`, `Filter`, `NestedLoopJoin`) are initialized in a nested hierarchy.
5. **Iterative Fetching**: The parent operator calls `.next()`, propagating down the tree. 
6. **Page Access**: Operators request pages from the `BufferPoolManager`. The `BufferPoolManager` caches disk pages into memory frames using a Least-Recently-Accessed (LRA) eviction strategy.
7. **MVCC Check**: The `TransactionManager` checks the visibility of slotted records using transaction IDs (`xmin`, `xmax`) against the session snapshot.
8. **Logging**: Writing operations append records to the `LogManager` WAL buffer before committing changes.
9. **Materialization**: Visible records are returned and rendered on the interactive console.

---

## 5. Page Format & Buffer Pool

### Slotted Page Layout
MiniDB simulates a slotted-page architecture inside 4KB pages.
- **Page Header**:
  - `pageId`: Unique identifier for the page.
  - `freeSpaceOffset`: Byte offset indicating the start of free space.
  - `slotCount`: Number of slot pointers in the directory.
- **Slot Directory**: Array of slots where each slot contains:
  - `offset`: Byte offset of the tuple in the page data area.
  - `size`: Serialized length of the tuple.
  - `active`: Boolean flag indicating if the record is active (or deleted).
- **Data Area**: Serialized JSON-equivalent byte layout growing from the end of the page toward the slot directory.

### Buffer Pool Manager
Maintains an array of `BufferFrame` structures representing memory cache slots:
- **Pin Count**: Tracks active operations using a page. A page cannot be evicted if `pinCount > 0`.
- **Dirty Flag**: Marks pages modified in memory. If a dirty page is evicted, it is flushed back to disk storage.
- **LRA Eviction**: When the pool is full, it evicts the unpinned page with the oldest access timestamp (`lastAccessed`).

---

## 6. B+ Tree Design

The primary index is implemented as a B+ Tree with order \(M = 3\).
- **Internal Nodes**: Contain route keys and pointers to child nodes. They guide point queries and range searches.
- **Leaf Nodes**: Contain keys and actual data payloads, which are Record IDs (`RID` containing `pageId` and `slotId`). Leaf nodes are linked sequentially to optimize range scans.
- **Dynamic Splits**: When a leaf or internal node exceeds the order capacity ($M$ keys), it triggers `splitChild()`. Keys are divided, and the median key is pushed up to the parent.
- **Deletions**: In this playground version, deletions flag the index key entry and the slot directory active flag.

---

## 7. Volcano Operator Execution

The physical execution engine follows the Volcano Iterator model:

```typescript
export interface Operator {
  init(): void;
  next(): Tuple | null;
  close(): void;
}
```

- **`SeqScan`**: Scans the heap page directory, fetching pages into the buffer pool frame-by-frame and retrieving slots.
- **`IndexScan`**: Traverses the B+ Tree index to locate the target `RID`, fetching only the single target page containing the tuple.
- **`Filter`**: Evaluates comparison predicates (e.g. `=`, `>`, `<`) against tuples returned by a child operator.
- **`NestedLoopJoin`**: Joins an outer relation and inner relation by scanning the inner relation for each tuple matching the join condition.

---

## 8. Cost-Based Selection & Join Ordering Formulas

The `CostBasedOptimizer` determines the optimal scan types and execution join orderings using table statistics and cost formulas:

### 1. Selectivity Factor ($S$)
* **Exact match query** (`=`): 
  $$S = \frac{1}{\max(1, \text{TableSize})}$$
* **Range query** (`>`, `<`): 
  $$S = 0.3$$

### 2. Estimated Rows ($R_{\text{est}}$)
$$R_{\text{est}} = \lceil \text{TableSize} \times S \rceil$$

### 3. Scan & Join Cost Selection
* **Sequential Scan Cost ($C_{\text{seq}}$):** 
  $$C_{\text{seq}} = \text{TableSize} \times 1.0$$
* **Index Scan Cost ($C_{\text{index}}$):** 
  $$C_{\text{index}} = 2.0 + (R_{\text{est}} \times 1.2)$$

If $C_{\text{index}} < C_{\text{seq}}$, the optimizer chooses `IndexScan`.

* **Nested Loop Join Order Costing:**
  To join Table A and Table B, the optimizer estimates the cost of both join order permutations:
  $$\text{Cost}(A \bowtie B) = C_{\text{scan}}(A) + R_{\text{outer}}(A) \times C_{\text{scan}}(B)$$
  $$\text{Cost}(B \bowtie A) = C_{\text{scan}}(B) + R_{\text{outer}}(B) \times C_{\text{scan}}(A)$$

The optimizer selects the permutation with the lower total cost. If Table B (Inner) is cheaper as the Outer, the execution engine dynamically swaps the relations, swaps predicate column indexes, and adjusts the projection output layout.

---

## 9. Extension Track — MVCC Concurrency

We selected **Track B — Concurrency** to implement Multi-Version Concurrency Control (MVCC) with Snapshot Isolation.

### Motivation
Standard Two-Phase Locking (2PL) relies on exclusive/shared locks to enforce serializability. However, 2PL introduces:
* High lock acquisition overhead and contention under heavy workloads.
* **Read-Write Blocking:** Readers block writers, and writers block readers, throttling concurrency.
* Cascading rollbacks and frequent deadlock aborts.

By replacing 2PL with MVCC, **readers never block writers, and writers never block readers**. Readers query a consistent transaction snapshot, completely avoiding locking delays.

### Design
1. **Tuple Versioning:** Every tuple contains metadata header fields:
   * `xmin`: Transaction ID that inserted the record version.
   * `xmax`: Transaction ID that deleted or updated the record version.
2. **Active Transaction Snapshot:** When starting a select query, the current transaction records a snapshot of all active transaction IDs (`snapshotActiveTxns`).
3. **Snapshot Isolation Visibility Rules:** A tuple is visible to a reading transaction if:
   * `xmin` is committed AND was not active when the reader's snapshot started.
   * `xmax` is either unassigned (`0`), aborted, active, or committed *after* the reader's snapshot started.

### Results
* **Elimination of Lock Delays:** Read queries proceed at instantaneous in-memory execution speed (~25,000 lookup ops/sec) regardless of concurrent write transactions.
* **Zero Deadlocks:** Since readers do not acquire shared locks, read-only and mixed transactions are completely immune to deadlock aborts.

---

## 10. MVCC Snapshot Visibility Logic

A reading transaction $T_{\text{reader}}$ evaluates visibility using the following checklist:

1. **Insertion Check (`xmin`)**:
   * If `xmin` is not committed (and is not $T_{\text{reader}}$), the tuple is **invisible**.
   * If `xmin` was active when the reader snapshot was taken (present in `activeSnapTxns`), the tuple is **invisible** (concurrent transaction).
2. **Deletion Check (`xmax`)**:
   * If `xmax` is `0`, the tuple is **visible**.
   * If `xmax` is set and committed (and was not active at the reader's snapshot initiation), the deletion is visible, meaning the tuple is **invisible**.
   * If `xmax` was active at snapshot initiation, the reader does not see the deletion, so the tuple remains **visible**.

---

## 11. WAL & ARIES Recovery

MiniDB logs write operations to a Write-Ahead Log (WAL) to ensure durability. The ARIES recovery protocol runs in three distinct phases following a crash:

1. **Analysis Phase**:
   * Scans the WAL forward from the beginning.
   * Identifies active transactions that did not commit before the crash (to be undone).
   * Reconstructs committed transaction IDs (`committedTxns`).
2. **Redo Phase**:
   * Replays all WAL history forward.
   * Restores modified tuple states to disk pages, bringing the database back to its exact state at the time of the crash ("repeating history").
3. **Undo Phase**:
   * Scans the WAL backward.
   * Reverses changes made by active or aborted transactions that were interrupted, restoring previous tuple versions to maintain database consistency.

---

## 12. Limitations

* **Simulated Disk**: Storage is backed by in-memory mock variables; it does not persist to physical file systems upon process exit.
* **SQL Parsing**: Supports simple queries (`SELECT`, `INSERT`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`) and single-predicate `WHERE` clauses.
* **B+ Tree Merging**: Deletions flag keys in leaves instead of performing key-redistribution and page merging.
* **Lock Escalation**: The dashboard runs single-threaded; session transactions emulate concurrent states through mock concurrency variables.

---

## 13. Benchmarks

MiniDB contains a performance suite that evaluates transaction throughput and query latencies:
* **Writes Throughput**: Evaluated by executing 1,000 sequential `INSERT` statements to measure transactions per second.
* **Reads Throughput**: Evaluated by running 2,000 point lookup select queries.
* **Latency comparison (Index vs Table Scan)**: Evaluates the retrieval cost of point select queries using the B+ Tree index vs. scanning the entire table.

### Benchmark Results (Typical Performance)
* **Writes Throughput**: ~8,000 op/s
* **Reads Throughput**: ~25,000 op/s
* **Index Scan Latency**: ~0.04 ms
* **Table Scan Latency**: ~1.20 ms
* *Result*: Index lookup is approximately **30x faster** than a full table scan on small datasets, scaling logarithmically on larger sets.

---

## 14. How to Run

Follow these steps to run the interactive, real-time live database engine visualization dashboard on your machine:

### 1. Install Dependencies
Navigate to the project directory and install the necessary package dependencies:
```bash
npm install
```

### 2. Start the Development Server
Launch the Vite development server:
```bash
npm run dev
```

### 3. Open the Dashboard
Open your web browser and navigate to the address output in the terminal (usually [http://localhost:5173](http://localhost:5173)). You can execute SQL queries, trigger database crashes, run ARIES crash recovery, and watch page splits and buffer pool frames change in real-time!
