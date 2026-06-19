# 04. Filesystem Interface

> [!NOTE]
> This document describes the public `Workspace.fs` surface and is kept
> in step with the code in `@cloudflare/dofs`. A few spots are
> explicitly flagged where the doc reflects an intended target (true
> streaming `writeFile`, mount-layer error codes); everything else is
> what ships today.

`Workspace.fs` is the file API. It's inspired by `node:fs/promises` for
familiarity — same method names, similar option shapes — but it's a much
smaller surface and it leans on `ReadableStream<Uint8Array>` wherever a
file could be large.

```ts
interface Workspace {
  fs:    WorkspaceFilesystem;
  shell: WorkspaceShell;        // see 05_shell_interface.md
}
```

Three things to keep in mind when porting Node code over:

- Every method is **async**, even ones Node ships as sync-only.
- Paths are **absolute** and POSIX-style (see
  [01. VFS](./01_vfs.md)).
- The default `readFile` return is a **stream**, not a Buffer. Pass
  `"utf8"` (or `{ encoding: "utf8" }`) when you actually want a string in
  memory. Use streams whenever the file could be larger than a few
  hundred KB — they pipe directly into `Response`, `fetch`, R2 `put`,
  and any other `ReadableStream` consumer without buffering.

See the [appendix](#appendix-comparison-with-nodefspromises) for a
method-by-method mapping against `node:fs/promises`.
## API

### `readFile`

```ts
readFile(path: string): Promise<ReadableStream<Uint8Array>>
readFile(path: string, encoding: "utf8"): Promise<string>
readFile(path: string, options: { encoding?: "utf8" }): Promise<string>
```

Defaulting to a stream is deliberate — most reads in an agent context
are "send this file somewhere" and never need to be in memory.

```ts
// Stream a large file straight to the client.
const stream = await fs.readFile("/workspace/build/out.wasm");
return new Response(stream, { headers: { "content-type": "application/wasm" } });

// Read a small text file into a string.
const todo = await fs.readFile("/workspace/notes/todo.md", "utf8");

// The verbose form, for symmetry with node:fs/promises.
const config = await fs.readFile("/workspace/config.json", { encoding: "utf8" });
```

### `writeFile`

```ts
writeFile(
  path:    string,
  content: string | Uint8Array | ReadableStream<Uint8Array>,
  options?: { mode?: number }
): Promise<void>
```

Accepts a stream so callers can supply uploads, R2 bodies, and `fetch`
responses without an intermediate `arrayBuffer()`. Stream sources are
consumed incrementally: bytes are re-windowed into fixed `CHUNK_SIZE`
(512 KiB) pieces, hashed, and staged into `vfs_blobs` as they arrive,
so peak memory is bounded by one chunk plus whatever the source
yields per pull — not the full file. The inode, dirent, chunk-list,
and manifest rows are committed in one short transaction once the
source drains; a mid-stream failure leaves orphan blob rows that
`gc()` reaps on its next pass.

```ts
// Text.
await fs.writeFile("/workspace/notes/todo.md", "- [ ] ship it\n");

// Binary.
await fs.writeFile("/workspace/data/blob.bin", new Uint8Array([1, 2, 3]));

// Supply an HTTP upload as a stream (consumed incrementally).
await fs.writeFile("/workspace/uploads/big.csv", request.body!);

// Pipe an R2 object into the workspace.
const obj = await env.BUCKET.get("imports/data.parquet");
if (obj) await fs.writeFile("/workspace/imports/data.parquet", obj.body);

// Mark a script executable.
await fs.writeFile("/workspace/bin/run.sh", "#!/bin/sh\necho hi\n", { mode: 0o755 });
```

### `rm`

```ts
rm(path: string, options?: { recursive?: true; force?: true }): Promise<void>
```

Replaces both `unlink` and `rmdir`. Pass `recursive: true` for non-empty
directories; `force: true` silences `ENOENT`.

> The `recursive?: true` / `force?: true` literal types are intentional
> today and reject `false`. Widening to `boolean` for `node:fs/promises`
> parity is a deferred follow-up.

```ts
// Single file.
await fs.rm("/workspace/notes/todo.md");

// Recursive directory wipe.
await fs.rm("/workspace/build", { recursive: true });

// Idempotent cleanup.
await fs.rm("/workspace/cache", { recursive: true, force: true });
```

### `mkdir`

```ts
mkdir(path: string, options?: { recursive?: true; mode?: number }): Promise<void>
```

Same literal-`true` caveat as `rm` — see the note above.

```ts
await fs.mkdir("/workspace/notes");
await fs.mkdir("/workspace/projects/a/b/c", { recursive: true });
```

### `readdir`

```ts
readdir(path: string): Promise<Array<{
  name:        string;
  parentPath:  string;
  isFile:      boolean;
  isDirectory: boolean;
}>>
```

Returns dirent-shaped entries by default so you don't need a follow-up
`stat()` to tell files from directories.

```ts
for (const entry of await fs.readdir("/workspace/notes")) {
  if (entry.isDirectory) console.log(`d ${entry.name}/`);
  else                   console.log(`f ${entry.name}`);
}
```

### `stat`

```ts
stat(path: string): Promise<{
  name:        string;
  mode:        number;
  mtime:       number;   // ms since epoch
  size:        number;
  isFile:      boolean;
  isDirectory: boolean;
}>
```

`name` is the last segment of the canonicalized path. For the workspace
root this is the empty string: `(await fs.stat("/")).name === ""`.

`stat` follows symlinks transparently; there is no `lstat`. See the
note on internal symlink support in the appendix.

> When a parent path segment is itself a file, `stat` reports `ENOENT`
> (because resolution returns `null` for that case) rather than
> `ENOTDIR`. `mkdir` and `writeFile` raise `ENOTDIR` explicitly for the
> same shape — see the error table.

```ts
const s = await fs.stat("/workspace/build/out.wasm");
console.log(`${s.size} bytes, modified ${new Date(s.mtime).toISOString()}`);
```

### `find`

```ts
find(
  directory: string,
  pattern?:  string,           // simple glob (`*.ts`, `**/*.md`)
): Promise<Array<{ path; type: "file" | "dir" }>>
```

Resolves `directory` first: throws `ENOENT` if the directory does not
exist and `ENOTDIR` if `directory` points at a file. The glob is
matched against each candidate's path **relative to `directory`**, not
its absolute path — so `**/*.ts` under `/workspace/src` matches
`a/b.ts`, not `/workspace/src/a/b.ts`.

Only `*`, `**`, and `**/` are honored; `?`, character classes, and
brace expansions are matched literally.

```ts
// Every TypeScript file in the project.
const ts = await fs.find("/workspace/src", "**/*.ts");

// Everything under a directory (no pattern).
const all = await fs.find("/workspace/notes");
```

### `ls`

```ts
ls(prefix: string): Promise<string[]>
```

Flat list of every file at or under `prefix`. The match is
**segment-aware**, not pure string-prefix: `ls("/workspace/notes")`
returns the file `/workspace/notes` (if it is a file) and every file
under `/workspace/notes/…`, but never `/workspace/notes-archive/x`.

Cheaper than `find` when you don't need the directory rows.

`ls` does **not** validate the prefix — a missing path returns `[]`
silently rather than throwing `ENOENT`. Use `stat` first if you need to
distinguish "empty directory" from "no such directory".

```ts
const paths = await fs.ls("/workspace/.agents/skills");
```

### `grep`

Available on `Workspace.fs` for parity with the agent tools, and on
`Workspace.shell` when you want it to run inside the container (faster
for large trees because it uses ripgrep).

```ts
grep(
  pattern: string,
  path:    string,
  options?: { ignoreCase?: boolean }
): Promise<{ path: string; line: number; text: string }[]>
```

`pattern` is a **literal substring** — not a regex, not a glob.
`ignoreCase` lowercases both sides before comparing.

`path` may be a directory **or a single file**. Directory walks return
matches in walk order. Each result row carries:

- `path` — absolute path of the matching file.
- `line` — 1-indexed line number within that file.
- `text` — the entire matching line (without the trailing newline), not
  just the matched substring.

```ts
const hits = await fs.grep("TODO", "/workspace/src", { ignoreCase: true });
for (const hit of hits) {
  console.log(`${hit.path}:${hit.line}: ${hit.text}`);
}
```

See [05. Shell Interface](./05_shell_interface.md) for the container-side
variant.

## Error handling

Errors thrown by `fs` are POSIX-style — a `NodeJS.ErrnoException`-shaped
object with a `code` property (and a `path` property where it applies) —
so handlers from Node code port over directly.

| Code | When |
| --- | --- |
| `ENOENT` | Path does not exist and `force` is not true. Also raised by `stat` when a parent segment turns out to be a file. |
| `ENOTEMPTY` | Path is a non-empty directory and `recursive` is not true. |
| `ENOTDIR` | A parent path segment is a file (raised explicitly by `mkdir` and `writeFile`; `find` raises it when its `directory` argument is a file). |
| `EISDIR` | Expected a file, got a directory (e.g. `readFile` on a dir, `writeFile` on `/`). |
| `EEXIST` | `mkdir` without `recursive: true` on an existing path. |
| `EINVAL` | Invalid path or unsupported options. |
| `ELOOP` | Symlink traversal exceeded 40 hops. Thrown by the internal resolver when the `node:vfs` adapter wires up a cycle. |
| `EPERM` | Operation is forbidden, e.g. deleting the workspace root. |
| `EIO` | Backing storage failed unexpectedly. |
| `EACCES` | *Reserved for future mount layer (see [06. Mount Interface](./06_mount_interface.md)).* No code path in `workspace-fs` currently throws it. |
| `EROFS` | *Reserved for future mount layer (see [06. Mount Interface](./06_mount_interface.md)).* No code path in `workspace-fs` currently throws it. |

### Example: handle "file missing" and bubble everything else

```ts
async function readConfig(): Promise<Config> {
  try {
    const text = await this.workspace.fs.readFile("/workspace/config.json", "utf8");
    return JSON.parse(text) as Config;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      // First boot: seed a default config and return it.
      const seed: Config = { version: 1, theme: "dark" };
      await this.workspace.fs.writeFile(
        "/workspace/config.json",
        JSON.stringify(seed, null, 2),
      );
      return seed;
    }
    // Anything else (EIO, ...) is a real problem — let it surface so
    // the agent's outer error handler logs it and the request fails
    // loudly.
    throw err;
  }
}
```

### Example: idempotent cleanup

```ts
// Equivalent to `rm -rf` — never throws on missing paths.
await this.workspace.fs.rm("/workspace/build", { recursive: true, force: true });
```

## Change events

`Workspace.fs` is the read/write surface; **change events** are the
*observe* surface. Instead of polling, a consumer subscribes once and is
pushed a notification whenever the tree mutates — to refresh a UI, kick
off indexing, or trigger any secondary work.

There are two consumer shapes:

- **In-process** — host Durable Object / Worker code subscribes directly
  to the local store via `Workspace.watchChanges(...)` (or the lower-level
  `subscribeChanges(db, ...)` from `@cloudflare/dofs`).
- **Over the wire** — an external client subscribes across the capnweb
  boundary via `SyncRPC.watchChanges(...)`; see
  [08. Capnweb Interface](./08_capnweb_interface.md).

```ts
const unsubscribe = workspace.watchChanges(
  (batch) => {
    for (const event of batch) {
      // event.op: "create" | "modify" | "chmod" | "rename"
      //         | "delete" | "subtree" | "resync"
      console.log(event.op, "path" in event ? event.path : `@rev ${event.rev}`);
    }
  },
  { path: "/workspace", ignore: ["**/node_modules/**"] },
);
// ...later
unsubscribe();
```

`Workspace.watchChanges` observes every mutation to the local store,
whether it originates host-side (a `Workspace.fs` write) or arrives from
the sandbox via `Workspace.pull` — the apply path runs through the same
primitives that publish events.

### Event model

Each event carries the `rev` (the monotonic revision counter; see
[02. Sync Protocol](./02_sync_protocol.md)) it was stamped at. The `op`
field is the sole discriminator:

| `op` | Payload | Meaning |
| --- | --- | --- |
| `create` | `path`, `meta` | A path came into existence (file, dir, symlink, or a new hardlink name). |
| `modify` | `path`, `meta` | An existing file's content or size changed. |
| `chmod` | `path`, `meta` | An existing path's mode changed; content untouched. |
| `rename` | `from`, `to`, `meta` | A path moved. Carries both endpoints. |
| `delete` | `path` | A path was removed. |
| `subtree` | `path` | Coalesced: something under `path` changed (opt-in; see `coalesceDirs`). Re-read the directory. |
| `resync` | — | The stream dropped events; reconcile from `rev`. See [The `resync` marker](#the-resync-marker). |

`meta` is intentionally **decoupled** from the sync wire's `ChangeEntry`:
it carries lean, consumer-oriented metadata — `{ type, mode, mtime,
size?, target? }` — and never chunk hashes. A consumer that needs file
contents reads them through `Workspace.fs` (in-process) or `fetchChanges`
(over the wire), not from the event.

### Options

| Option | Default | Effect |
| --- | --- | --- |
| `path` | `"/"` | Directory scope. Only changes inside it are delivered. |
| `recursive` | `true` | When `false`, only direct children of `path`. |
| `ignore` | `[]` | Glob (`**/node_modules/**`, `*.log`) or whole-segment (`node_modules`) patterns to drop. Matched paths and their subtrees are never delivered. |
| `coalesceDirs` | `[]` | Directories whose subtree changes collapse to a single `subtree` event instead of per-file events. Deepest match wins for nested entries. |
| `window` | `0` | Batch window in ms. `0` delivers one batch per committing transaction; a positive value accumulates across transactions and delivers on a timer. |
| `maxBufferedEvents` | `1000` | Cap on buffered events before the buffer collapses to a single `resync` (see below). |

### Delivery semantics

- **Best-effort, in-incarnation.** Subscriptions are in-memory and live
  only for the current Durable Object incarnation; they do not survive
  hibernation. Delivery is not guaranteed (see `resync`).
- **Ordered.** Events are delivered in commit order. Listeners receive
  batches (`ChangeEvent[]`); a batch is never empty.
- **Coalesced.** Repeated touches of one path within a single committed
  transaction collapse to one event. With `coalesceDirs`, an entire
  subtree's churn collapses to one `subtree` event per delivery.

### Performance and overhead

Emission adds work only on the write path, and only when at least one
subscriber is attached.

- **Zero-subscriber cost.** Publishing is gated on subscriber presence —
  a single registry check returns early when nobody is listening. The
  default workload, including all sandbox-side writes, pays effectively
  nothing.
- **With subscribers.** Each committing transaction coalesces its touched
  paths in memory (`O(paths touched)`) and materialises each distinct
  path once — a small indexed read, the same query the sync layer
  already runs, and cheaper than the sync wire's `ChangeEntry` because no
  chunk hashes are read. Cost is bounded by the number of distinct paths
  in the transaction, not by bytes written or write-syscall count.
- **No write amplification.** Emission never inserts rows; there is no
  durable event log. It is pure read + in-memory dispatch.
- **Fan-out** is `O(subscribers)` for the filter/enqueue step; the
  per-path materialise is computed once and shared across subscribers.

> The sandbox (`wsd`) is deliberately **not** wired as a change-event
> consumer: it is the dominant writer and already receives upstream state
> through the pull-based sync loop, so subscribing it would only add
> echo and cost on the hot FUSE write path.

### High-volume bursts

A burst of thousands of files — `npm install`, `rm -rf node_modules` — is
the scaling concern. Because the sandbox write buffer flushes per file
close, such a burst is thousands of *separate* committing transactions,
so it is tamed at delivery time rather than by per-transaction
coalescing:

- **Windowed batching (`window`).** A positive window accumulates events
  across transactions and delivers one coalesced `ChangeEvent[]` batch,
  so a UI re-renders once per window instead of thousands of times.
- **Directory coalescing (`coalesceDirs`).** Folds a known-noisy subtree
  (`/node_modules`, `/dist`) into one `subtree` event per window —
  `O(directories touched)` rather than `O(files)`.
- **Overflow → `resync`.** When a burst still exceeds
  `maxBufferedEvents`, the buffer collapses to a single `resync` marker
  rather than growing without bound (see below).

Producer-side work stays `O(distinct paths)`; what crosses the wire and
reaches the consumer is bounded regardless of burst size.

### The `resync` marker

- **What it is.** `{ op: "resync", rev }` is a *control* event, not a
  filesystem fact. It means: *some individual events were dropped; the
  live stream is no longer complete up to `rev`.* It is not an error and
  does not say what changed — only that the consumer's view may be stale
  at or before `rev`.
- **Why it exists.** Push delivery is best-effort by design, because the
  three alternatives are all worse: blocking the filesystem write path
  to slow a lagging consumer is unacceptable; buffering without bound
  risks unbounded memory and wire traffic under a burst or a stalled
  consumer; and silently dropping events would leave the consumer's view
  wrong. `resync` is the fourth option — shed load *and* stay correct by
  handing the consumer back to the authoritative, durable record. Push
  gives **liveness**; the durable pull gives **completeness**; `resync`
  is the explicit handoff between them.
- **When it fires.** A per-subscriber buffer overflow (a huge
  single-transaction delete, or a slow consumer that can't keep up), and
  the equivalent reconnect-after-disconnect or post-hibernation cases —
  all land the consumer in the same recovery path.
- **How to handle it.** Track the last `rev` you processed; on `resync`,
  reconcile against the source of truth, then resume the live stream.
  Over the wire that means a `fetchChanges` sweep from your last rev:

  ```ts
  let lastRev = 0;
  for await (const batch of stream) {
    for (const event of batch) {
      if (event.op === "resync") {
        using caughtUp = await sync.fetchChanges({ after: { rev: lastRev, path: null } });
        await applyAuthoritative(caughtUp.stream); // reconcile your view
        lastRev = event.rev;
        continue;
      }
      applyEvent(event);
      lastRev = event.rev;
    }
  }
  ```

  In-process, the local store *is* the source of truth, so reconciling
  means re-reading the watched scope directly (`readdir` / `stat`) rather
  than calling `fetchChanges`. Either way it is the **same routine** a
  consumer already needs after a reconnect or hibernation — one code
  path, not a special case.

### Lifecycle

Change-event subscriptions exist only for the current Durable Object
incarnation. After hibernation or a dropped connection the subscription
is gone; a reconnecting consumer re-subscribes and reconciles from its
last-seen `rev` — the same `resync` recovery above. See
[11. Lifecycle](./11_lifecycle.md).

## Appendix: comparison with `node:fs/promises`

For reference, here's the public surface of `node:fs/promises` and how it
maps to `Workspace.fs`:

| `node:fs/promises` | `Workspace.fs` | Notes |
| --- | --- | --- |
| `readFile` | `readFile` | Stream by default; pass `"utf8"` for a string. |
| `writeFile` | `writeFile` | Accepts `string`, `Uint8Array`, or `ReadableStream` (consumed incrementally). |
| `appendFile` | — | Read, concat, write. Not a primitive. |
| `mkdir` | `mkdir` | `{ recursive: true }` supported. |
| `rmdir` | `rm` | One method for files and dirs (matches modern Node). |
| `rm` | `rm` | `{ recursive: true }` for non-empty dirs. |
| `unlink` | `rm` | Same. |
| `readdir` | `readdir` | Always returns dirent-shaped entries. |
| `stat` / `lstat` | `stat` | No `lstat`; `stat` follows symlinks. See note below. |
| `truncate` | — | Read, slice, write. |
| `chmod` | — | Pass `mode` to `writeFile` / `mkdir` at create time. There is no way to chmod an existing file without rewriting its bytes. |
| `chown` | — | No ownership model. |
| `utimes` | — | `mtime` is managed by the VFS. |
| `cp` / `copyFile` | — | Read + write. |
| `rename` | — | Read + write + delete. |
| `realpath` | — | Paths are already canonical. |
| `symlink` / `readlink` | — | Not on the public surface; see note below. |
| `watch` | `watchChanges` | Push-based change events on the local store (see [Change events](#change-events)). A lower-level polling primitive also lives in `fs/watch.ts` (`createWatcher`, `createWatchAsyncIterable`), not exposed on the `WorkspaceFilesystem` class. |
| `open` / `FileHandle` | — | Use streams instead. |
| `glob` | `find` | Limited glob support (`*`, `**`, `**/` only). |
| — | `grep` | Not in `node:fs`; included here for agents. Substring match. |
| — | `find` | Recursive directory walk with an optional glob, relative-rooted. |
| — | `ls` | Flat list of file paths under a directory (segment-aware). |

### Note: symlinks

Symlinks exist as an **internal primitive** used by the `node:vfs`
adapter — the schema supports a `'symlink'` node type with a
`link_target`, and the resolver in `fs/resolve.ts` follows them with a
40-hop cap (throws `ELOOP` on overflow). They are **not** part of the
public `WorkspaceFilesystem` surface: there are no `fs.symlink` or
`fs.readlink` methods on `Workspace.fs`, and callers should treat all
visible paths as if they pointed straight at real files.
