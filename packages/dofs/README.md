# `@cloudflare/dofs`

> [!IMPORTANT]
> **PREVIEW ONLY** This package is provided as a preview for feedback only.
> APIs are unstable and the design is subject to change.
>
> Suitable for experiments, exploration and prototypes. It is NOT suitable
> for production use at this time.
>
> The specification under [`docs/`](docs/) is forward-looking — read it for
> intent, not as description of the code today.

Durable Object SQLite-backed virtual filesystem for Cloudflare Workspace.

This package exposes a JavaScript module, not a CLI. It bundles three layers that can be used independently:

- A `Database` wrapper around Durable Object SQL storage plus `initializeSchema` for the `vfs_*` tables.
- Filesystem primitives under `src/fs/*` (`mkdir`, `writeFile`, `readFile`, `rm`, `readdir`, `stat`, `lstat`, `chmod`, `find`, `ls`, `grep`, `symlink`, `readlink`, `gc`, `watch`) operating on a `Database`.
- `SQLiteWorkspaceProvider`, a `@platformatic/vfs` adapter that composes those primitives into a node-shaped filesystem (fd table, positional `readSync`/`writeSync`, `watchSync`, symlinks). This is what `wsd` mounts via FUSE.
- Sync protocol building blocks operating on the same `Database`: `applyChanges`, `stageBlob`, `materialiseChange`, `coalesceChanges`, `fetchChanges`, `fetchObjects`, `hasObjects`, `pushObjects`, `buildManifest`, `currentRev`, `compareChangeCursors`, `readWatermark`/`writeWatermark`, `assertAppliedPushCursor`, and `DEFAULT_IGNORE`/`isIgnored`. The wire wiring lives in `@cloudflare/workspace-rpc`.
- Push-based change events via `subscribeChanges`. The wire wiring lives in `@cloudflare/workspace-rpc`.

## Change events

`subscribeChanges(db, listener, options?)` delivers a push notification
whenever the filesystem mutates, so external consumers can refresh a UI
or trigger secondary work without polling.

```ts
import { subscribeChanges } from "@cloudflare/dofs";

const unsubscribe = subscribeChanges(
  db,
  (batch) => {
    for (const event of batch) {
      // event.op is "create" | "modify" | "chmod" | "rename" |
      // "delete" | "subtree" | "resync".
      console.log(event.op, "path" in event ? event.path : "");
    }
  },
  {
    path: "/src", // scope (default "/")
    recursive: true, // default true
    ignore: ["**/node_modules/**", "*.log"], // glob or whole-segment
    coalesceDirs: ["/node_modules"], // fold a noisy subtree to one event
    window: 50, // batch window in ms (0 = synchronous, default)
    maxBufferedEvents: 1000, // overflow → one "resync" marker
  },
);
```

Each event carries the `rev` it was stamped at. Events are decoupled
from the sync wire's `ChangeEntry`: they carry lean `ChangeMeta` (type,
mode, mtime, size, symlink target) rather than chunk hashes. A consumer
that needs content reads it through the normal fs API or `fetchChanges`.

Delivery is **best-effort and lives only for the current Durable Object
incarnation.** Subscriptions are in-memory; they do not survive
hibernation, and events emitted while a consumer is disconnected (or
after a buffer overflow, signalled by a `resync` marker) are dropped.
Emission is gated on subscriber presence, so the write path pays nothing
when nobody is listening.

### Handling `resync`

A `resync` event means "some events were dropped; your view may be stale
at or before `rev`." It is the deliberate trade that keeps push delivery
from either blocking writes or buffering without bound under a burst.
Recover by reconciling against the source of truth from your last-seen
`rev`, then resume:

```ts
let lastRev = 0;
const unsubscribe = subscribeChanges(db, (batch) => {
  for (const event of batch) {
    if (event.op === "resync") {
      // Local store is authoritative here: re-read the watched scope.
      // (Over the RPC boundary, call sync.fetchChanges({ after: lastRev })
      // instead — see docs/04 → Change events.)
      reconcile(db, lastRev);
      lastRev = event.rev;
      continue;
    }
    applyEvent(event);
    lastRev = event.rev;
  }
});
```

This is the same routine a consumer needs after a reconnect or
hibernation, so it is one code path, not a special case. See
[`docs/04_filesystem_interface.md` → Change events](../../docs/04_filesystem_interface.md#change-events)
for the full contract.

Minimal DO-side usage — initialize the schema; the `Database` becomes the handle every other helper takes:

```ts
import { Database, initializeSchema } from "@cloudflare/dofs";

export class WorkspaceDO extends DurableObject {
  private readonly db: Database;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.db = new Database(ctx.storage);
    initializeSchema(this.db, Date.now);
  }
}
```

> The `src/fs/*` primitives (`mkdir`, `writeFile`, `readFile`, `rm`, `readdir`, `stat`, `find`, `ls`, `grep`, `symlink`, `readlink`, `gc`, `watch`) are not re-exported from the package root yet — they are consumed in-tree by `SQLiteWorkspaceProvider` and by the sync `applyChanges` path. On the node side, instantiate `SQLiteWorkspaceProvider` (the `@platformatic/vfs` adapter) for a familiar node:fs-shaped surface; this is what `@cloudflare/workspace-wsd` mounts via FUSE. A higher-level DO-side `Workspace` class with the `fs`/`shell`/`push`/`pull` surface described in [`../../docs/README.md`](../../docs/README.md) is still future work.

## Implementation status


- `Database` wrapper around Durable Object SQL storage in place.
- Schema initialization for the documented `vfs_*` tables (FS and sync) implemented and split into `schema/core.ts` + `schema/sync.ts`.
- `incrementRev()` shared sequencer in place. FS writes stamp the returned value into `vfs_nodes.rev` and pass it to `sync/changes.ts` for tombstones.
- `SQLiteTestStorage` (backed by `node:sqlite`) available from `./testing` for unit tests against a real in-memory database; `RecordingStorage` available from the package root for workerd-safe schema assertions.
- All filesystem primitives listed above are implemented and unit-tested.
- `SQLiteWorkspaceProvider` (the `@platformatic/vfs` adapter) implemented and exported from the package entrypoint; consumed by `@cloudflare/workspace-wsd`.
- Buffered-write surface for the FUSE driver: `createFileSync`,
  `writeRangeSync`, `truncateFileSync`, `readRangeSync`, `chmodSync`,
  `openWriteBufferSync`, `openWriteBufferForCreateSync`, and
  `releaseWriteBufferSync` on the provider. The driver opens a buffer
  on FUSE create/open, mutates it through subsequent writes and
  truncates, and commits chunks in one transaction at release time.
  Reads against the same database see the buffered bytes immediately.
- Content-addressed blob cache: `readFile`, `readRangeSync`,
  `provider.readFileSync`, and the partial-chunk read-modify-write
  helper share a per-`Database` LRU keyed by `vfs_blob_bytes.hash`.
  Repeated reads of dedup'd chunks (a file of zeroes, a re-used
  package payload) skip SQLite after the first fetch.
- Sync protocol building blocks implemented and exported; the typed RPC surface on top of them lives in `@cloudflare/workspace-rpc`.
