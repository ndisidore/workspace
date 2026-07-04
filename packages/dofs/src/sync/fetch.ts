import type { Database } from "../storage.js";
import type { ChangeEntry } from "./changes.js";
import { coalesceChanges } from "./coalesce.js";
import { pushObjects } from "./push.js";
import type { ChangeCursor } from "./watermarks.js";

// The fetch wire is the mirror of the push wire: same SQL,
// opposite direction. The DO calls fetchChanges / fetchObjects on
// the container; the container calls push / pushObjects on the DO.
// Both names exist so call sites read in their own direction.

export function fetchChanges(
  db: Database,
  after: ChangeCursor | number,
  options: { ignore?: string[] } = {},
): AsyncIterable<ChangeEntry> {
  return coalesceChanges(db, after, options);
}

export function fetchObjects(
  db: Database,
  hashes: Uint8Array[],
): AsyncIterable<{ hash: Uint8Array; bytes: Uint8Array }> {
  return pushObjects(db, hashes);
}

// Encode a hash to match SQLite's uppercase hex(), so a BLOB list can
// travel through a JSON array — JSON can't carry raw bytes.
function toHexUpper(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out.toUpperCase();
}

// Subset-test the input hashes against vfs_blobs. Symmetric on both
// sides: the DO probes the container before pushObjects, and the
// container probes the DO before fetchObjects, so both sides ship
// only the bytes the receiver lacks.
//
// Single SQL round-trip: the hashes are hex-encoded into a JSON array
// and matched against hex(hash) through json_each. Present hashes are
// returned in input order, preserving any duplicates the caller
// passed.
export function hasObjects(db: Database, hashes: Uint8Array[]): Uint8Array[] {
  if (hashes.length === 0) return [];
  const wanted = hashes.map(toHexUpper);
  const present = new Set(
    db
      .all<{ hex: string }>(
        "SELECT hex(hash) AS hex FROM vfs_blobs WHERE hex(hash) IN (SELECT value FROM json_each(?))",
        JSON.stringify(wanted),
      )
      .map((row) => row.hex),
  );
  return hashes.filter((_, i) => present.has(wanted[i]));
}
