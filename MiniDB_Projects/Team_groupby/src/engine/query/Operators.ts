import { Tuple } from '../types';
import { BufferPoolManager } from '../storage/BufferPoolManager';
import { PageManager } from '../storage/PageManager';

export interface Operator {
  init(): void;
  next(): Tuple | null;
  close(): void;
}

export class SeqScan implements Operator {
  private tuples: Tuple[];
  private cursor: number = 0;

  constructor(tuples: Tuple[]) {
    this.tuples = tuples;
  }

  init() {
    this.cursor = 0;
  }

  next(): Tuple | null {
    if (this.cursor < this.tuples.length) {
      return this.tuples[this.cursor++];
    }
    return null;
  }

  close() {}
}

export class IndexScan implements Operator {
  private bufferPool: BufferPoolManager;
  private pageId: number;
  private slotId: number;
  private filterPredicate?: (t: Tuple) => boolean;
  private cursor: number = 0;

  constructor(
    bufferPool: BufferPoolManager,
    pageId: number,
    slotId: number,
    filterPredicate?: (t: Tuple) => boolean
  ) {
    this.bufferPool = bufferPool;
    this.pageId = pageId;
    this.slotId = slotId;
    this.filterPredicate = filterPredicate;
  }

  init() {
    this.cursor = 0;
  }

  next(): Tuple | null {
    if (this.pageId === -1 || this.slotId === -1) return null;
    if (this.cursor === 0) {
      this.cursor++;
      const page = this.bufferPool.fetchPage(this.pageId);
      const tuple = PageManager.getTuple(page, this.slotId);
      this.bufferPool.unpinPage(this.pageId, false);
      if (tuple && (!this.filterPredicate || this.filterPredicate(tuple))) {
        return tuple;
      }
    }
    return null;
  }

  close() {}
}

export class Filter implements Operator {
  private child: Operator;
  private predicate: (t: Tuple) => boolean;

  constructor(child: Operator, predicate: (t: Tuple) => boolean) {
    this.child = child;
    this.predicate = predicate;
  }

  init() {
    this.child.init();
  }

  next(): Tuple | null {
    let t: Tuple | null;
    while ((t = this.child.next()) !== null) {
      if (this.predicate(t)) {
        return t;
      }
    }
    return null;
  }

  close() {
    this.child.close();
  }
}

export class NestedLoopJoin implements Operator {
  private outer: Operator;
  private inner: Operator;
  private joinPredicate: (outer: Tuple, inner: Tuple) => boolean;
  private currentOuter: Tuple | null = null;

  constructor(outer: Operator, inner: Operator, predicate: (o: Tuple, i: Tuple) => boolean) {
    this.outer = outer;
    this.inner = inner;
    this.joinPredicate = predicate;
  }

  init() {
    this.outer.init();
    this.inner.init();
    this.currentOuter = this.outer.next();
  }

  next(): Tuple | null {
    while (this.currentOuter !== null) {
      let innerTuple = this.inner.next();
      if (innerTuple === null) {
        this.inner.init();
        this.currentOuter = this.outer.next();
        continue;
      }
      if (this.joinPredicate(this.currentOuter, innerTuple)) {
        return {
          id: this.currentOuter.id,
          values: [...this.currentOuter.values, ...innerTuple.values],
          xmin: this.currentOuter.xmin,
          xmax: this.currentOuter.xmax
        };
      }
    }
    return null;
  }

  close() {
    this.outer.close();
    this.inner.close();
  }
}
