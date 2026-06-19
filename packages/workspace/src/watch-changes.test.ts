import type { ChangeEvent } from "@cloudflare/dofs";
import { SQLiteTestStorage } from "@cloudflare/dofs/testing";
import { describe, expect, it } from "vitest";

import { Workspace } from "./workspace.js";

function makeStorage(): SQLiteTestStorage {
  return new SQLiteTestStorage();
}

describe("Workspace.watchChanges", () => {
  it("delivers change events for local filesystem writes", async () => {
    const ws = new Workspace({ storage: makeStorage() });
    const events: ChangeEvent[] = [];
    const unsubscribe = ws.watchChanges((batch) => events.push(...batch));
    try {
      await ws.fs.writeFile("/a.txt", "hello");
      await ws.fs.writeFile("/a.txt", "hello again");
      expect(events).toHaveLength(2);
      expect(events[0]).toMatchObject({ op: "create", path: "/a.txt" });
      expect(events[1]).toMatchObject({ op: "modify", path: "/a.txt" });
    } finally {
      unsubscribe();
    }
  });

  it("stops delivering after unsubscribe", async () => {
    const ws = new Workspace({ storage: makeStorage() });
    const events: ChangeEvent[] = [];
    const unsubscribe = ws.watchChanges((batch) => events.push(...batch));
    await ws.fs.mkdir("/before");
    unsubscribe();
    await ws.fs.mkdir("/after");
    expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/before"]);
  });

  it("honors scope and ignore options", async () => {
    const ws = new Workspace({ storage: makeStorage() });
    await ws.fs.mkdir("/src");
    await ws.fs.mkdir("/node_modules");
    const events: ChangeEvent[] = [];
    const unsubscribe = ws.watchChanges((batch) => events.push(...batch), {
      path: "/src",
      ignore: ["**/node_modules/**"],
    });
    try {
      await ws.fs.writeFile("/src/index.ts", "x");
      await ws.fs.writeFile("/node_modules/pkg.js", "y");
      await ws.fs.writeFile("/outside.txt", "z");
      expect(events.map((e) => ("path" in e ? e.path : e.op))).toEqual(["/src/index.ts"]);
    } finally {
      unsubscribe();
    }
  });
});
