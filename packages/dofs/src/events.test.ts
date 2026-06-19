import { describe, expect, it, vi } from "vitest";

import { type ChangeEvent, publishChange, subscribeChanges } from "./events.js";
import { chmod } from "./fs/chmod.js";
import { link } from "./fs/link.js";
import { mkdir } from "./fs/mkdir.js";
import { rename } from "./fs/rename.js";
import { rm } from "./fs/rm.js";
import { symlink } from "./fs/symlink.js";
import { withDB } from "./fs/with-db.js";
import { writeFileSync } from "./fs/writeFile.js";
import type { Database } from "./storage.js";
import { applyChangesSync } from "./sync/apply.js";

const now = () => 2000;

// A few tests below drive an *explicit* multi-operation
// db.transactionSync wrapper to exercise per-transaction coalescing
// and rollback discard. That nests one fs primitive's transaction
// inside another, which issues a SQLite SAVEPOINT — fine under the
// node:sqlite test backend, but rejected by the Durable Object
// runtime (state.storage.transactionSync forbids SAVEPOINT). The
// production code never nests this way (see sync/apply.ts), so these
// behaviours are only reachable, and only meaningful, under the node
// backend. Skip them under workerd rather than assert an environment
// limitation.
const underWorkerd =
  typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers";

function encode(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

// Collect every event delivered to a default (synchronous, window=0)
// subscription. With window 0 each committing transaction delivers one
// batch immediately, so `events` is populated by the time a mutation
// call returns.
function collect(db: Database, options = {}): ChangeEvent[] {
  const events: ChangeEvent[] = [];
  subscribeChanges(db, (batch) => events.push(...batch), options);
  return events;
}

describe("change events — operations", () => {
  it("emits a create for mkdir", async () => {
    await withDB((db) => {
      const events = collect(db);
      mkdir(db, "/a", { mode: 0o755 }, now);
      expect(events).toEqual([
        {
          op: "create",
          rev: expect.any(Number),
          path: "/a",
          meta: { type: "dir", mode: 0o755, mtime: 2000 },
        },
      ]);
    });
  });

  it("emits a create for each directory a recursive mkdir makes", async () => {
    await withDB((db) => {
      const events = collect(db);
      mkdir(db, "/x/y/z", { recursive: true }, now);
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/x", "/x/y", "/x/y/z"]);
      expect(events.every((e) => e.op === "create")).toBe(true);
    });
  });

  it("emits a create for a new file and modify for an overwrite", async () => {
    await withDB((db) => {
      const events = collect(db);
      writeFileSync(db, "/f.txt", encode("hello"), {}, now);
      writeFileSync(db, "/f.txt", encode("longer content"), {}, now);
      expect(events).toHaveLength(2);
      expect(events[0]).toMatchObject({ op: "create", path: "/f.txt" });
      expect(events[1]).toMatchObject({ op: "modify", path: "/f.txt" });
      // File metadata reflects the post-write state.
      expect(events[1]).toMatchObject({ meta: { type: "file", size: "longer content".length } });
    });
  });

  it("emits a chmod distinct from a modify", async () => {
    await withDB((db) => {
      writeFileSync(db, "/f.txt", encode("hi"), {}, now);
      const events = collect(db);
      chmod(db, "/f.txt", 0o600, now);
      expect(events).toEqual([
        {
          op: "chmod",
          rev: expect.any(Number),
          path: "/f.txt",
          meta: { type: "file", mode: 0o600, mtime: 2000, size: 2 },
        },
      ]);
    });
  });

  it("emits a create for a symlink with its target", async () => {
    await withDB((db) => {
      const events = collect(db);
      symlink(db, "/target", "/link", now);
      expect(events).toEqual([
        {
          op: "create",
          rev: expect.any(Number),
          path: "/link",
          meta: { type: "symlink", mode: 0o777, mtime: 2000, target: "/target" },
        },
      ]);
    });
  });

  it("emits a create for a new hardlink name", async () => {
    await withDB((db) => {
      writeFileSync(db, "/a.txt", encode("data"), {}, now);
      const events = collect(db);
      link(db, "/a.txt", "/b.txt");
      expect(events).toEqual([
        {
          op: "create",
          rev: expect.any(Number),
          path: "/b.txt",
          meta: { type: "file", mode: expect.any(Number), mtime: expect.any(Number), size: 4 },
        },
      ]);
    });
  });

  it("emits a delete for rm of a file", async () => {
    await withDB((db) => {
      writeFileSync(db, "/f.txt", encode("x"), {}, now);
      const events = collect(db);
      rm(db, "/f.txt", {});
      expect(events).toEqual([{ op: "delete", rev: expect.any(Number), path: "/f.txt" }]);
    });
  });

  it("emits a delete per path for a recursive rm", async () => {
    await withDB((db) => {
      mkdir(db, "/d", {}, now);
      writeFileSync(db, "/d/a", encode("a"), {}, now);
      writeFileSync(db, "/d/b", encode("b"), {}, now);
      const events = collect(db);
      rm(db, "/d", { recursive: true });
      const deleted = events.map((e) => ("path" in e ? e.path : e.op)).sort();
      expect(events.every((e) => e.op === "delete")).toBe(true);
      expect(deleted).toEqual(["/d", "/d/a", "/d/b"]);
    });
  });

  it("emits a single rename carrying both endpoints", async () => {
    await withDB((db) => {
      writeFileSync(db, "/old.txt", encode("data"), {}, now);
      const events = collect(db);
      rename(db, "/old.txt", "/new.txt");
      expect(events).toEqual([
        {
          op: "rename",
          rev: expect.any(Number),
          from: "/old.txt",
          to: "/new.txt",
          meta: { type: "file", mode: expect.any(Number), mtime: expect.any(Number), size: 4 },
        },
      ]);
    });
  });
});

// These exercise the storage.ts commit/discard wiring (the bus flushes
// queued descriptors on the outermost commit and drops them on a throw)
// using a single, non-nested outer transaction with a directly-queued
// descriptor. That keeps them runnable on the Durable Object runtime,
// which forbids the nested SAVEPOINT the per-primitive tests below need.
describe("change events — commit and discard", () => {
  it("delivers a queued descriptor when the outer transaction commits", async () => {
    await withDB((db) => {
      mkdir(db, "/x", {}, now);
      const events = collect(db);
      db.transactionSync(() => {
        publishChange(db, { op: "chmod", path: "/x" });
      });
      expect(events).toEqual([
        { op: "chmod", rev: expect.any(Number), path: "/x", meta: expect.any(Object) },
      ]);
    });
  });

  it("drops a queued descriptor when the outer transaction throws", async () => {
    await withDB((db) => {
      mkdir(db, "/x", {}, now);
      const events = collect(db);
      expect(() =>
        db.transactionSync(() => {
          publishChange(db, { op: "chmod", path: "/x" });
          throw new Error("boom");
        }),
      ).toThrow("boom");
      expect(events).toEqual([]);
    });
  });
});

describe.skipIf(underWorkerd)("change events — transaction semantics", () => {
  it("emits nothing when the transaction rolls back", async () => {
    await withDB((db) => {
      const events = collect(db);
      expect(() =>
        db.transactionSync(() => {
          writeFileSync(db, "/f.txt", encode("hi"), {}, now);
          throw new Error("boom");
        }),
      ).toThrow("boom");
      expect(events).toEqual([]);
    });
  });

  it("flushes once on the outer commit and coalesces per path", async () => {
    await withDB((db) => {
      const batches: ChangeEvent[][] = [];
      subscribeChanges(db, (batch) => batches.push(batch));
      db.transactionSync(() => {
        writeFileSync(db, "/f.txt", encode("one"), {}, now);
        writeFileSync(db, "/f.txt", encode("two-longer"), {}, now);
      });
      // One delivery for the whole outer transaction, one coalesced
      // event for the path (create then modify collapses to create).
      expect(batches).toHaveLength(1);
      expect(batches[0]).toEqual([
        { op: "create", rev: expect.any(Number), path: "/f.txt", meta: expect.any(Object) },
      ]);
    });
  });

  it("reports the source delete when a rename's destination is removed in the same transaction", async () => {
    await withDB((db) => {
      writeFileSync(db, "/a.txt", encode("data"), {}, now);
      const events = collect(db);
      db.transactionSync(() => {
        rename(db, "/a.txt", "/b.txt");
        rm(db, "/b.txt", {});
      });
      const summary = events.map((e) => ("path" in e ? `${e.op}:${e.path}` : e.op)).sort();
      // Without folding the rename into the per-path model the consumer
      // learns only about /b.txt and keeps a stale /a.txt forever.
      expect(summary).toContain("delete:/a.txt");
    });
  });

  it("keeps committed sibling work when a nested savepoint rolls back", async () => {
    await withDB((db) => {
      const events = collect(db);
      db.transactionSync(() => {
        mkdir(db, "/keep", {}, now);
        try {
          db.transactionSync(() => {
            mkdir(db, "/drop", {}, now);
            throw new Error("inner");
          });
        } catch {
          // swallow; the outer transaction continues
        }
      });
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/keep"]);
    });
  });
});

describe("change events — subscription lifecycle", () => {
  it("stops delivering after unsubscribe", async () => {
    await withDB((db) => {
      const events: ChangeEvent[] = [];
      const unsubscribe = subscribeChanges(db, (batch) => events.push(...batch));
      mkdir(db, "/a", {}, now);
      unsubscribe();
      mkdir(db, "/b", {}, now);
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/a"]);
    });
  });
});

describe("change events — scope and ignore", () => {
  it("filters to the watched directory, non-recursive", async () => {
    await withDB((db) => {
      mkdir(db, "/a", {}, now);
      mkdir(db, "/a/sub", {}, now);
      const events = collect(db, { path: "/a", recursive: false });
      writeFileSync(db, "/a/direct.txt", encode("x"), {}, now);
      writeFileSync(db, "/a/sub/nested.txt", encode("y"), {}, now);
      writeFileSync(db, "/elsewhere.txt", encode("z"), {}, now);
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/a/direct.txt"]);
    });
  });

  it("drops paths matched by an ignore glob", async () => {
    await withDB((db) => {
      mkdir(db, "/node_modules", {}, now);
      const events = collect(db, { ignore: ["**/node_modules/**"] });
      writeFileSync(db, "/node_modules/pkg.js", encode("x"), {}, now);
      writeFileSync(db, "/src.ts", encode("y"), {}, now);
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/src.ts"]);
    });
  });
});

describe("change events — coalesceDirs", () => {
  // A batch window collapses changes from several separate
  // transactions into one delivery, where coalesceDirs folds the
  // subtree. This avoids wrapping multiple writes in one explicit
  // transaction (unsupported under the Durable Object runtime), so
  // these run on both test backends.
  it("collapses many changes under a coalesce dir into one subtree event", async () => {
    vi.useFakeTimers();
    try {
      await withDB((db) => {
        mkdir(db, "/node_modules", {}, now);
        const batches: ChangeEvent[][] = [];
        subscribeChanges(db, (batch) => batches.push(batch), {
          coalesceDirs: ["/node_modules"],
          window: 50,
        });
        writeFileSync(db, "/node_modules/a.js", encode("a"), {}, now);
        writeFileSync(db, "/node_modules/b.js", encode("b"), {}, now);
        writeFileSync(db, "/node_modules/c.js", encode("c"), {}, now);
        vi.advanceTimersByTime(50);
        expect(batches).toHaveLength(1);
        expect(batches[0]).toEqual([
          { op: "subtree", rev: expect.any(Number), path: "/node_modules" },
        ]);
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("collapses independently per coalesce dir and leaves others per-file", async () => {
    vi.useFakeTimers();
    try {
      await withDB((db) => {
        mkdir(db, "/node_modules", {}, now);
        mkdir(db, "/dist", {}, now);
        mkdir(db, "/src", {}, now);
        const batches: ChangeEvent[][] = [];
        subscribeChanges(db, (batch) => batches.push(batch), {
          coalesceDirs: ["/node_modules", "/dist"],
          window: 50,
        });
        writeFileSync(db, "/node_modules/a.js", encode("a"), {}, now);
        writeFileSync(db, "/dist/bundle.js", encode("b"), {}, now);
        writeFileSync(db, "/src/index.ts", encode("c"), {}, now);
        vi.advanceTimersByTime(50);
        expect(batches).toHaveLength(1);
        const summary = batches[0].map((e) => `${e.op}:${"path" in e ? e.path : ""}`).sort();
        expect(summary).toEqual(["create:/src/index.ts", "subtree:/dist", "subtree:/node_modules"]);
      });
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("change events — burst handling", () => {
  it("falls back to a single resync marker on buffer overflow", async () => {
    await withDB((db) => {
      mkdir(db, "/d", {}, now);
      writeFileSync(db, "/d/a", encode("a"), {}, now);
      writeFileSync(db, "/d/b", encode("b"), {}, now);
      writeFileSync(db, "/d/c", encode("c"), {}, now);
      const events = collect(db, { maxBufferedEvents: 2 });
      // Recursive rm deletes /d/a, /d/b, /d/c and /d in one
      // transaction — four events, over the cap of two.
      rm(db, "/d", { recursive: true });
      expect(events).toEqual([{ op: "resync", rev: expect.any(Number) }]);
    });
  });

  it("batches across transactions within the window", async () => {
    vi.useFakeTimers();
    try {
      await withDB((db) => {
        const batches: ChangeEvent[][] = [];
        subscribeChanges(db, (batch) => batches.push(batch), { window: 50 });
        mkdir(db, "/a", {}, now);
        mkdir(db, "/b", {}, now);
        // Nothing delivered until the window elapses.
        expect(batches).toHaveLength(0);
        vi.advanceTimersByTime(50);
        expect(batches).toHaveLength(1);
        expect(batches[0].map((e) => ("path" in e ? e.path : e.op))).toEqual(["/a", "/b"]);
      });
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("change events — backend apply path", () => {
  // Backend-originated changes arrive through applyChangesSync, which
  // mixes fs primitives (mkdir/writeFile/symlink/rm, which publish) with
  // direct structural surgery (a raw dir mode update, and a tombstone-
  // free subtree removal). A subscriber must still observe every
  // resulting mutation; otherwise Workspace.watchChanges silently misses
  // upstream changes.

  it("emits a chmod for an upstream mode change to an existing directory", async () => {
    await withDB((db) => {
      mkdir(db, "/d", { mode: 0o755 }, now);
      const events = collect(db);
      applyChangesSync(
        db,
        [{ kind: "dir", rev: 99, path: "/d", mode: 0o700, mtime: 3 }],
        new Map(),
      );
      expect(events).toEqual([
        {
          op: "chmod",
          rev: expect.any(Number),
          path: "/d",
          meta: { type: "dir", mode: 0o700, mtime: 3 },
        },
      ]);
    });
  });

  it("emits child deletes when a directory is replaced by a file", async () => {
    await withDB((db) => {
      mkdir(db, "/dst/sub", { recursive: true }, now);
      writeFileSync(db, "/dst/sub/old.txt", encode("old"), {}, now);
      const events = collect(db);
      applyChangesSync(
        db,
        [{ kind: "file", rev: 99, path: "/dst", mode: 0o644, mtime: 3, size: 0, chunks: [] }],
        new Map(),
      );
      const summary = events.map((e) => `${e.op}:${"path" in e ? e.path : e.op}`).sort();
      // The replaced subtree's children disappear; /dst becomes a file
      // (delete-then-create on /dst collapses to create).
      expect(summary).toEqual(["create:/dst", "delete:/dst/sub", "delete:/dst/sub/old.txt"]);
    });
  });

  it("emits child deletes when a directory is replaced by a symlink", async () => {
    await withDB((db) => {
      mkdir(db, "/dst/sub", { recursive: true }, now);
      writeFileSync(db, "/dst/sub/old.txt", encode("old"), {}, now);
      const events = collect(db);
      applyChangesSync(
        db,
        [{ kind: "symlink", rev: 99, path: "/dst", mode: 0o777, mtime: 3, target: "/t" }],
        new Map(),
      );
      const summary = events.map((e) => `${e.op}:${"path" in e ? e.path : e.op}`).sort();
      expect(summary).toEqual(["create:/dst", "delete:/dst/sub", "delete:/dst/sub/old.txt"]);
    });
  });
});

describe("change events — rename coalescing", () => {
  it("collapses a rename fully contained in a coalesce dir to one subtree", async () => {
    await withDB((db) => {
      mkdir(db, "/node_modules", {}, now);
      writeFileSync(db, "/node_modules/old.js", encode("x"), {}, now);
      const events = collect(db, { coalesceDirs: ["/node_modules"] });
      rename(db, "/node_modules/old.js", "/node_modules/new.js");
      expect(events).toEqual([{ op: "subtree", rev: expect.any(Number), path: "/node_modules" }]);
    });
  });

  it("delivers a boundary-crossing rename whole instead of collapsing it", async () => {
    await withDB((db) => {
      mkdir(db, "/node_modules", {}, now);
      mkdir(db, "/src", {}, now);
      writeFileSync(db, "/node_modules/pkg", encode("x"), {}, now);
      const events = collect(db, { coalesceDirs: ["/node_modules"] });
      rename(db, "/node_modules/pkg", "/src/pkg");
      expect(events).toEqual([
        {
          op: "rename",
          rev: expect.any(Number),
          from: "/node_modules/pkg",
          to: "/src/pkg",
          meta: expect.any(Object),
        },
      ]);
    });
  });

  it("preserves the in-scope endpoint of a rename out of an out-of-scope coalesce dir", async () => {
    await withDB((db) => {
      mkdir(db, "/node_modules", {}, now);
      mkdir(db, "/src", {}, now);
      writeFileSync(db, "/node_modules/pkg", encode("x"), {}, now);
      // A watcher on /src must learn /src/pkg appeared and must NOT get a
      // subtree event for /node_modules, which it is not watching.
      const events = collect(db, { path: "/src", coalesceDirs: ["/node_modules"] });
      rename(db, "/node_modules/pkg", "/src/pkg");
      expect(events.some((e) => e.op === "subtree")).toBe(false);
      expect(events.some((e) => "to" in e && e.to === "/src/pkg")).toBe(true);
    });
  });
});
