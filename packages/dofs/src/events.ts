// Push-based file change events.
//
// Every filesystem mutation tags the operation it performs by calling
// publishChange() inside its transaction. The Database queues those
// descriptors and, when the *outer* transaction commits, hands them
// here to be materialised into ChangeEvents and dispatched to every
// subscriber. Work rolled back never reaches a subscriber.
//
// Delivery is best-effort and lives only for the current Durable
// Object incarnation: subscriptions are in-memory and do not survive
// hibernation. Each event carries the rev it was stamped at, so a
// consumer that misses events (disconnect, eviction, buffer overflow)
// recovers by pulling from its last-seen rev through the durable
// fetchChanges path. The bus never persists anything.
//
// See docs and the change-event plan for the delivery contract;
// the matching scope semantics are shared with src/fs/watch.ts.

import { resolveInode } from "./fs/resolve.js";
import { isInScope } from "./fs/watch.js";
import { canonicalizePath } from "./path.js";
import type { Database } from "./storage.js";
import { isIgnored } from "./sync/ignore.js";
import { currentRev } from "./sync/watermarks.js";

// Lean, consumer-oriented metadata for a path. Deliberately decoupled
// from the sync wire's ChangeEntry: no chunk hashes, no inline bytes.
// A consumer that needs content reads it through the normal fs API or
// the durable fetchChanges surface.
export interface ChangeMeta {
  type: "file" | "dir" | "symlink";
  mode: number;
  mtime: number;
  // Present only for files.
  size?: number;
  // Present only for symlinks.
  target?: string;
}

// A single change delivered to a subscriber. `op` is the sole
// discriminator and carries the operation specificity the materialised
// state cannot — `chmod` is distinct from `modify`, and `rename` keeps
// the from→to link a state-only model would lose.
//
//   create  — a path came into existence (file, dir, symlink, or a new
//             hardlink name for an existing inode).
//   modify  — an existing file's content or size changed.
//   chmod   — an existing path's mode changed; content untouched.
//   rename  — a path moved from `from` to `to`.
//   delete  — a path was removed.
//   subtree — coalesced: something under `path` changed. Emitted only
//             when the subscriber opts into coalesceDirs. The consumer
//             re-reads the directory.
//   resync  — the subscriber's buffer overflowed and individual events
//             were dropped. The consumer pulls from its last-seen rev
//             through fetchChanges, then resumes.
export type ChangeEvent =
  | { op: "create"; rev: number; path: string; meta: ChangeMeta }
  | { op: "modify"; rev: number; path: string; meta: ChangeMeta }
  | { op: "chmod"; rev: number; path: string; meta: ChangeMeta }
  | { op: "rename"; rev: number; from: string; to: string; meta: ChangeMeta }
  | { op: "delete"; rev: number; path: string }
  | { op: "subtree"; rev: number; path: string }
  | { op: "resync"; rev: number };

// What a mutation primitive queues. Renames carry both endpoints; the
// rest carry the single affected path. These never escape the package
// — they are materialised into ChangeEvents at commit time.
export type ChangeDescriptor =
  | { op: "create" | "modify" | "chmod" | "delete"; path: string }
  | { op: "rename"; from: string; to: string };

type PathOp = "create" | "modify" | "chmod" | "delete";

export type ChangeListener = (batch: ChangeEvent[]) => void;

export interface SubscribeChangesOptions {
  // Directory scope. Defaults to "/". Only changes inside this
  // directory are delivered.
  path?: string;
  // Recurse into subdirectories. Defaults to true.
  recursive?: boolean;
  // Glob or whole-segment patterns to drop. Matched paths (and their
  // subtrees) are never delivered. See sync/ignore.ts.
  ignore?: string[];
  // Directories whose subtree changes collapse to a single `subtree`
  // event per delivery instead of per-file events. The deepest match
  // wins when entries nest.
  coalesceDirs?: string[];
  // Batch window in milliseconds. 0 (default) delivers one batch per
  // committing transaction synchronously. A positive value accumulates
  // events across transactions and delivers on a timer.
  window?: number;
  // Cap on buffered events before the buffer collapses to a single
  // `resync` marker. Bounds memory and wire traffic under bursts.
  maxBufferedEvents?: number;
}

// Publish a change from a mutation primitive. A no-op when the bus has
// no subscribers, so the hot write path pays nothing unless someone is
// listening. Must be called inside a Database transaction: the queued
// descriptor flushes only when the outer transaction commits and is
// discarded on rollback.
export function publishChange(db: Database, descriptor: ChangeDescriptor): void {
  db.changeBus.enqueue(descriptor);
}

// Subscribe to change events. Returns an unsubscribe function. The
// listener receives batches; a batch is never empty.
export function subscribeChanges(
  db: Database,
  listener: ChangeListener,
  options: SubscribeChangesOptions = {},
): () => void {
  return db.changeBus.subscribe(listener, options);
}

// Per-Database event bus. Owns the pending-descriptor queue and the
// subscriber set. The Database drives commit()/discard()/mark()/
// rollbackTo() from its transaction machinery.
export class ChangeBus {
  #subscribers = new Set<Subscriber>();
  #pending: ChangeDescriptor[] = [];

  get hasSubscribers(): boolean {
    return this.#subscribers.size > 0;
  }

  // Queue a descriptor. Cheap presence gate first: with no subscribers
  // we allocate nothing and never materialise.
  enqueue(descriptor: ChangeDescriptor): void {
    if (this.#subscribers.size === 0) return;
    this.#pending.push(descriptor);
  }

  // Savepoint bookkeeping so a rolled-back nested transaction drops
  // exactly the descriptors it queued.
  mark(): number {
    return this.#pending.length;
  }

  rollbackTo(mark: number): void {
    if (mark < this.#pending.length) this.#pending.length = mark;
  }

  // Outer transaction committed: materialise the queued descriptors
  // and dispatch. Never throws — best-effort delivery must not turn a
  // listener or materialise failure into a mutation failure.
  commit(db: Database): void {
    const pending = this.#pending;
    this.#pending = [];
    if (pending.length === 0 || this.#subscribers.size === 0) return;
    let events: ChangeEvent[];
    try {
      events = materialiseDescriptors(db, pending);
    } catch {
      return;
    }
    if (events.length === 0) return;
    for (const subscriber of this.#subscribers) {
      subscriber.deliver(events);
    }
  }

  // Outer transaction rolled back: drop everything queued.
  discard(): void {
    this.#pending = [];
  }

  subscribe(listener: ChangeListener, options: SubscribeChangesOptions): () => void {
    const subscriber = new Subscriber(listener, options);
    this.#subscribers.add(subscriber);
    return () => {
      if (this.#subscribers.delete(subscriber)) {
        subscriber.dispose();
      }
    };
  }
}

// Turn a transaction's queued descriptors into the events a subscriber
// sees. Per-path coalescing collapses repeated touches to one event
// (the strongest op wins); renames pass through with the moved path's
// current metadata. All events share the transaction's rev.
function materialiseDescriptors(db: Database, descriptors: ChangeDescriptor[]): ChangeEvent[] {
  const rev = currentRev(db);
  const order: string[] = [];
  const byPath = new Map<string, PathOp>();
  const renames: { from: string; to: string }[] = [];

  for (const descriptor of descriptors) {
    if (descriptor.op === "rename") {
      renames.push({ from: descriptor.from, to: descriptor.to });
      continue;
    }
    const prior = byPath.get(descriptor.path);
    if (prior === undefined) order.push(descriptor.path);
    byPath.set(descriptor.path, combineOps(prior, descriptor.op));
  }

  const events: ChangeEvent[] = [];
  for (const { from, to } of renames) {
    const meta = materialiseMeta(db, to);
    if (meta !== null) {
      events.push({ op: "rename", rev, from, to, meta });
      continue;
    }
    // The destination no longer exists at commit — it was deleted or
    // renamed onward later in the same transaction. The source still
    // disappeared, so report that much rather than dropping the rename
    // entirely and leaving the consumer with a stale `from` forever.
    events.push({ op: "delete", rev, path: from });
  }
  for (const path of order) {
    const op = byPath.get(path) as PathOp;
    if (op === "delete") {
      events.push({ op: "delete", rev, path });
      continue;
    }
    const meta = materialiseMeta(db, path);
    // The path was created and removed within the same transaction
    // without an explicit delete descriptor — nothing to report.
    if (meta === null) continue;
    events.push({ op, rev, path, meta });
  }
  return events;
}

// Coalesce two ops on the same path within one transaction. delete is
// terminal unless the path is recreated; otherwise the higher-impact
// op wins (create > modify > chmod) so a create-then-modify collapses
// to create.
function combineOps(prior: PathOp | undefined, next: PathOp): PathOp {
  if (next === "delete") return "delete";
  if (prior === undefined) return next;
  if (prior === "delete") return next === "create" ? "create" : "delete";
  const rank = { chmod: 1, modify: 2, create: 3 } as const;
  return rank[next] > rank[prior] ? next : prior;
}

function materialiseMeta(db: Database, path: string): ChangeMeta | null {
  const node = resolveInode(db, path, { followSymlinks: false });
  if (node === null) return null;
  if (node.type === "symlink") {
    return { type: "symlink", mode: node.mode, mtime: node.mtime, target: node.linkTarget ?? "" };
  }
  if (node.type === "dir") {
    return { type: "dir", mode: node.mode, mtime: node.mtime };
  }
  return { type: "file", mode: node.mode, mtime: node.mtime, size: node.size };
}

// One subscription. Applies scope + ignore filtering, optional
// directory coalescing, a batch window, and a bounded buffer that
// collapses to a resync marker on overflow.
class Subscriber {
  readonly #listener: ChangeListener;
  readonly #scopePath: string;
  readonly #scopePrefix: string;
  readonly #recursive: boolean;
  readonly #ignore: string[];
  readonly #coalesceDirs: string[];
  readonly #window: number;
  readonly #maxBuffer: number;

  #buffer: ChangeEvent[] = [];
  // Maps a coalesce directory to its event's index in #buffer so
  // repeated subtree hits in one window collapse to one entry.
  #subtreeIndex = new Map<string, number>();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #overflowed = false;

  constructor(listener: ChangeListener, options: SubscribeChangesOptions) {
    this.#listener = listener;
    this.#scopePath = canonicalizePath(options.path ?? "/").path;
    this.#scopePrefix = this.#scopePath === "/" ? "/" : `${this.#scopePath}/`;
    this.#recursive = options.recursive !== false;
    this.#ignore = options.ignore ?? [];
    // Deepest directory first so the most specific coalesce dir wins.
    this.#coalesceDirs = (options.coalesceDirs ?? [])
      .map((dir) => canonicalizePath(dir).path)
      .sort((a, b) => b.length - a.length);
    this.#window = options.window ?? 0;
    this.#maxBuffer = options.maxBufferedEvents ?? 1000;
  }

  deliver(events: ChangeEvent[]): void {
    for (const event of events) this.#consider(event);
    if (this.#buffer.length > 0) this.#schedule();
  }

  dispose(): void {
    if (this.#timer !== undefined) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
  }

  #consider(event: ChangeEvent): void {
    if (this.#overflowed) return;
    const paths = pathsOf(event);
    if (paths.length === 0) return;
    if (!paths.some((p) => isInScope(p, this.#scopePath, this.#scopePrefix, this.#recursive))) {
      return;
    }
    if (paths.every((p) => isIgnored(p, this.#ignore))) return;
    const dir = this.#coalesceDirFor(paths);
    if (dir !== null) {
      this.#pushSubtree(dir, event.rev);
      return;
    }
    this.#push(event);
  }

  // A coalesce dir applies only when it contains *every* path the event
  // touches. A single-path event collapses as before; a rename collapses
  // only when both endpoints live under the same dir. A rename that
  // crosses the boundary (one endpoint inside, one outside) is delivered
  // whole instead, so the consumer never loses the endpoint that is not
  // under the coalesce dir — and a watcher never receives a subtree event
  // for a directory outside its own scope.
  #coalesceDirFor(paths: string[]): string | null {
    for (const dir of this.#coalesceDirs) {
      const prefix = dir === "/" ? "/" : `${dir}/`;
      if (paths.every((path) => path === dir || path.startsWith(prefix))) return dir;
    }
    return null;
  }

  #pushSubtree(dir: string, rev: number): void {
    const existing = this.#subtreeIndex.get(dir);
    if (existing !== undefined) {
      const current = this.#buffer[existing];
      if (current.op === "subtree" && rev > current.rev) {
        this.#buffer[existing] = { op: "subtree", rev, path: dir };
      }
      return;
    }
    this.#push({ op: "subtree", rev, path: dir }, dir);
  }

  #push(event: ChangeEvent, subtreeDir?: string): void {
    this.#buffer.push(event);
    if (subtreeDir !== undefined) {
      this.#subtreeIndex.set(subtreeDir, this.#buffer.length - 1);
    }
    if (this.#buffer.length > this.#maxBuffer) {
      this.#buffer = [{ op: "resync", rev: event.rev }];
      this.#subtreeIndex.clear();
      this.#overflowed = true;
    }
  }

  #schedule(): void {
    if (this.#window <= 0) {
      this.#flush();
      return;
    }
    if (this.#timer !== undefined) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#flush();
    }, this.#window);
    this.#timer.unref?.();
  }

  #flush(): void {
    if (this.#buffer.length === 0) return;
    const batch = this.#buffer;
    this.#buffer = [];
    this.#subtreeIndex.clear();
    this.#overflowed = false;
    try {
      this.#listener(batch);
    } catch {
      // Best-effort: a listener that throws must not poison the bus or
      // other subscribers.
    }
  }
}

function pathsOf(event: ChangeEvent): string[] {
  if (event.op === "rename") return [event.from, event.to];
  if (event.op === "resync") return [];
  return [event.path];
}
