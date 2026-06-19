import { describe, expect, it } from "vitest";

import { isIgnored } from "./ignore.js";

describe("isIgnored", () => {
  const list = ["node_modules", ".next", "target"];

  it("returns false for paths that don't intersect the list", () => {
    expect(isIgnored("/src/index.ts", list)).toBe(false);
    expect(isIgnored("/README.md", list)).toBe(false);
    expect(isIgnored("/", list)).toBe(false);
  });

  it("matches the segment exactly, not as a substring", () => {
    expect(isIgnored("/node_modules", list)).toBe(true);
    expect(isIgnored("/node_modules_old", list)).toBe(false);
    expect(isIgnored("/my_node_modules", list)).toBe(false);
  });

  it("matches anywhere in the path", () => {
    expect(isIgnored("/a/b/node_modules", list)).toBe(true);
    expect(isIgnored("/a/b/node_modules/c.js", list)).toBe(true);
    expect(isIgnored("/packages/x/node_modules/y/index.js", list)).toBe(true);
  });

  it("matches nested ignored dirs too", () => {
    expect(isIgnored("/a/.next/cache", list)).toBe(true);
    expect(isIgnored("/rust/target/debug/foo", list)).toBe(true);
  });

  it("returns false for an empty list", () => {
    expect(isIgnored("/anywhere/node_modules", [])).toBe(false);
  });

  describe("glob patterns", () => {
    it("matches a suffix glob anywhere in the tree", () => {
      const globs = ["*.log"];
      expect(isIgnored("/a/b/server.log", globs)).toBe(true);
      expect(isIgnored("/server.log", globs)).toBe(true);
      expect(isIgnored("/a/server.log.txt", globs)).toBe(false);
    });

    it("treats single * as segment-bounded", () => {
      const globs = ["/build/*"];
      expect(isIgnored("/build/output", globs)).toBe(true);
      // A single star does not cross a slash, but a matched directory
      // still hides its subtree.
      expect(isIgnored("/build/output/nested", globs)).toBe(true);
      expect(isIgnored("/src/build/output", globs)).toBe(false);
    });

    it("treats ** as crossing segments", () => {
      const globs = ["**/node_modules/**"];
      expect(isIgnored("/node_modules/x", globs)).toBe(true);
      expect(isIgnored("/a/b/node_modules/c/d.js", globs)).toBe(true);
      expect(isIgnored("/a/b/c.js", globs)).toBe(false);
    });

    it("matches a directory named by a ** prefix and its subtree", () => {
      const globs = ["**/node_modules"];
      expect(isIgnored("/node_modules", globs)).toBe(true);
      expect(isIgnored("/a/node_modules", globs)).toBe(true);
      expect(isIgnored("/a/node_modules/pkg/index.js", globs)).toBe(true);
    });

    it("anchors a leading-slash glob at the root", () => {
      const globs = ["/dist/**"];
      expect(isIgnored("/dist/bundle.js", globs)).toBe(true);
      expect(isIgnored("/dist", globs)).toBe(true);
      expect(isIgnored("/packages/x/dist/bundle.js", globs)).toBe(false);
    });

    it("honors ? as a single non-slash character", () => {
      const globs = ["/a/?.txt"];
      expect(isIgnored("/a/b.txt", globs)).toBe(true);
      expect(isIgnored("/a/bc.txt", globs)).toBe(false);
    });

    it("mixes glob and whole-segment patterns in one list", () => {
      const globs = ["node_modules", "*.log"];
      expect(isIgnored("/a/node_modules/x", globs)).toBe(true);
      expect(isIgnored("/a/debug.log", globs)).toBe(true);
      expect(isIgnored("/a/src/index.ts", globs)).toBe(false);
    });

    it("keeps matching correctly after the compiled-pattern cache evicts", () => {
      // Compile well past the cache ceiling so the earliest pattern is
      // evicted, then re-match it: a miss recompiles, so correctness
      // must not depend on the cache retaining the entry.
      for (let i = 0; i < 2000; i++) isIgnored("/x", [`p${i}`]);
      expect(isIgnored("/build", ["build"])).toBe(true);
      expect(isIgnored("/p0", ["p0"])).toBe(true);
      expect(isIgnored("/keep", ["drop"])).toBe(false);
    });
  });
});
