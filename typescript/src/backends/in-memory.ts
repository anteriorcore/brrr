import type { Cache, MemKey, Store } from "../store.ts";
import type { Publisher, Subscriber } from "../emitter.ts";
import { EventEmitter } from "node:events";
import type { Call } from "../call.ts";
import { BrrrTaskDoneEventSymbol } from "../symbol.ts";

export class InMemoryStore implements Store {
  private store = new Map<string, Uint8Array>();

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.keyToString(key);
    const value = this.store.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      return false;
    }
    this.store.delete(keyStr);
    return true;
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.keyToString(key);
    const current = this.store.get(keyStr);
    if (!current || !this.isEqualBytes(current, expected)) {
      return false;
    }
    this.store.set(keyStr, value);
    return true;
  }

  public async delete(key: MemKey): Promise<void> {
    const keyStr = this.keyToString(key);
    this.store.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array | undefined> {
    const keyStr = this.keyToString(key);
    return this.store.get(keyStr);
  }

  public async getWithRetry(key: MemKey): Promise<Uint8Array | undefined> {
    return this.get(key);
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.keyToString(key);
    return this.store.has(keyStr);
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.keyToString(key);
    this.store.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<boolean> {
    const keyStr = this.keyToString(key);
    if (this.store.has(keyStr)) {
      return false;
    }
    this.store.set(keyStr, value);
    return true;
  }

  private keyToString(key: MemKey): string {
    return `${key.type}/${key.callHash}`;
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((it, i) => it === b[i]);
  }
}

export class InMemoryCache implements Cache {
  private readonly cache = new Map<string, number>();

  public async incr(key: string): Promise<number> {
    const next = (this.cache.get(key) ?? 0) + 1;
    this.cache.set(key, next);
    return next;
  }
}

export class InMemoryEmitter implements Publisher, Subscriber {
  private readonly emitter = new EventEmitter();
  private readonly eventEmitter = new EventEmitter();

  public on(topic: string, listener: (callId: string) => void): void {
    this.emitter.on(topic, listener);
  }

  public onEventSymbol(
    event: typeof BrrrTaskDoneEventSymbol,
    listener: (call: Call) => void,
  ): void {
    this.eventEmitter.on(event, listener);
  }

  public async emit(topic: string, callId: string): Promise<void> {
    this.emitter.emit(topic, callId);
  }

  public async emitEventSymbol(
    event: typeof BrrrTaskDoneEventSymbol,
    call: Call,
  ): Promise<void> {
    this.eventEmitter.emit(event, call);
  }
}
