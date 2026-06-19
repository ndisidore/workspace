import { describe, expect, it } from "vitest";

import { clampWatchOptions } from "./server.js";

// watchChanges takes SubscribeChangesOptions straight off the wire, so
// the server must clamp the resource knobs an untrusted client controls.
describe("clampWatchOptions", () => {
  it("applies producer-side defaults when the caller omits the knobs", () => {
    const out = clampWatchOptions({});
    expect(out.window).toBe(50);
    expect(out.maxBufferedEvents).toBe(1000);
  });

  it("clamps an oversized buffer request to the server ceiling", () => {
    const out = clampWatchOptions({ maxBufferedEvents: 1_000_000 });
    expect(out.maxBufferedEvents).toBe(10_000);
  });

  it("clamps an oversized window request to the server ceiling", () => {
    const out = clampWatchOptions({ window: 10 * 60_000 });
    expect(out.window).toBe(60_000);
  });

  it("floors a non-positive buffer request at one event", () => {
    expect(clampWatchOptions({ maxBufferedEvents: 0 }).maxBufferedEvents).toBe(1);
    expect(clampWatchOptions({ maxBufferedEvents: -5 }).maxBufferedEvents).toBe(1);
  });

  it("preserves a window of zero (synchronous per-transaction delivery)", () => {
    expect(clampWatchOptions({ window: 0 }).window).toBe(0);
  });

  it("passes through scope and filter options untouched", () => {
    const out = clampWatchOptions({
      path: "/src",
      recursive: false,
      ignore: ["**/node_modules/**"],
      coalesceDirs: ["/dist"],
    });
    expect(out.path).toBe("/src");
    expect(out.recursive).toBe(false);
    expect(out.ignore).toEqual(["**/node_modules/**"]);
    expect(out.coalesceDirs).toEqual(["/dist"]);
  });

  it("falls back to the floor when handed NaN", () => {
    expect(clampWatchOptions({ window: Number.NaN }).window).toBe(0);
    expect(clampWatchOptions({ maxBufferedEvents: Number.NaN }).maxBufferedEvents).toBe(1);
  });
});
