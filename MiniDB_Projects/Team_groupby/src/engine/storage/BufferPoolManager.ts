import { Page, BufferFrame, StorageProvider } from '../types';
import { PageManager } from './PageManager';

export class BrowserStorageProvider implements StorageProvider {
  getItem(key: string): string | null {
    if (typeof localStorage !== 'undefined') {
      return localStorage.getItem(key);
    }
    return null;
  }
  setItem(key: string, value: string): void {
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem(key, value);
    }
  }
  removeItem(key: string): void {
    if (typeof localStorage !== 'undefined') {
      localStorage.removeItem(key);
    }
  }
  clear(): void {
    if (typeof localStorage !== 'undefined') {
      localStorage.clear();
    }
  }
}

export class InMemoryStorageProvider implements StorageProvider {
  private store = new Map<string, string>();
  getItem(key: string): string | null {
    return this.store.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    this.store.set(key, value);
  }
  removeItem(key: string): void {
    this.store.delete(key);
  }
  clear(): void {
    this.store.clear();
  }
}

export class BufferPoolManager {
  private frames: BufferFrame[] = [];
  private pool: Map<number, Page> = new Map();
  private diskPages: Map<number, Page> = new Map();
  public storageProvider: StorageProvider;

  constructor(size: number = 8, storageProvider?: StorageProvider) {
    for (let i = 0; i < size; i++) {
      this.frames.push({
        frameId: i,
        pageId: null,
        pinCount: 0,
        isDirty: false,
        lastAccessed: 0
      });
    }
    this.storageProvider = storageProvider || (typeof localStorage !== 'undefined' ? new BrowserStorageProvider() : new InMemoryStorageProvider());
    this.loadFromStorage();
  }

  getFrames(): BufferFrame[] {
    return this.frames;
  }

  getDiskPages(): Map<number, Page> {
    return this.diskPages;
  }

  fetchPage(pageId: number): Page {
    if (this.pool.has(pageId)) {
      const frame = this.frames.find(f => f.pageId === pageId)!;
      frame.pinCount++;
      frame.lastAccessed = Date.now();
      return this.pool.get(pageId)!;
    }

    let targetFrame = this.frames.find(f => f.pageId === null);
    if (!targetFrame) {
      targetFrame = this.findEvictionCandidate();
      if (!targetFrame) {
        throw new Error("Buffer pool full! All pages are pinned.");
      }
      this.evictFrame(targetFrame);
    }

    let page = this.diskPages.get(pageId);
    if (!page) {
      page = PageManager.createEmptyPage(pageId);
      this.diskPages.set(pageId, page);
      this.saveToStorage();
    }

    this.pool.set(pageId, page);
    targetFrame.pageId = pageId;
    targetFrame.pinCount = 1;
    targetFrame.isDirty = false;
    targetFrame.lastAccessed = Date.now();

    return page;
  }

  unpinPage(pageId: number, isDirty: boolean) {
    const frame = this.frames.find(f => f.pageId === pageId);
    if (frame) {
      if (frame.pinCount > 0) frame.pinCount--;
      if (isDirty) {
        frame.isDirty = true;
        const page = this.pool.get(pageId);
        if (page) {
          this.diskPages.set(pageId, page);
          this.saveToStorage();
        }
      }
    }
  }

  flushAll() {
    for (const frame of this.frames) {
      if (frame.pageId !== null && frame.isDirty && frame.pinCount === 0) {
        this.evictFrame(frame);
      }
    }
  }

  public clearStorage() {
    this.storageProvider.removeItem('minidb_disk_pages');
    this.diskPages.clear();
    this.pool.clear();
    for (const frame of this.frames) {
      frame.pageId = null;
      frame.pinCount = 0;
      frame.isDirty = false;
      frame.lastAccessed = 0;
    }
  }

  private loadFromStorage() {
    const saved = this.storageProvider.getItem('minidb_disk_pages');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        for (const [keyStr, val] of Object.entries(parsed)) {
          this.diskPages.set(Number(keyStr), val as Page);
        }
      } catch (e) {
        console.error("Failed to load disk pages from storage provider", e);
      }
    }
  }

  private saveToStorage() {
    const obj: Record<number, Page> = {};
    this.diskPages.forEach((val, key) => {
      obj[key] = val;
    });
    this.storageProvider.setItem('minidb_disk_pages', JSON.stringify(obj));
  }

  private findEvictionCandidate(): BufferFrame | undefined {
    let candidate: BufferFrame | undefined = undefined;
    let minTime = Infinity;
    for (const frame of this.frames) {
      if (frame.pinCount === 0 && frame.lastAccessed < minTime) {
        minTime = frame.lastAccessed;
        candidate = frame;
      }
    }
    return candidate;
  }

  private evictFrame(frame: BufferFrame) {
    if (frame.pageId === null) return;
    if (frame.isDirty) {
      const page = this.pool.get(frame.pageId)!;
      this.diskPages.set(frame.pageId, page);
      this.saveToStorage();
    }
    this.pool.delete(frame.pageId);
    frame.pageId = null;
    frame.isDirty = false;
    frame.pinCount = 0;
  }
}
