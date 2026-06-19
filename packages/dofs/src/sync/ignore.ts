// Path matcher for the container-side ignore list. The container
// uses this to drop paths from coalesceChanges before they hit the
// wire; the DO's Workspace.fs surface uses the same helper to make
// ignored paths invisible to API consumers; and the change-event
// emitter reuses it to filter and coalesce subscriber streams.
//
// Every pattern is a glob matched against the segment-relative path:
// `*` matches within a single segment, `**` matches across segments
// (including zero), `?` matches one non-slash character. A leading
// slash anchors at the root; without it the pattern matches at any
// depth. A pattern that names a directory also matches everything
// beneath it, so "**/node_modules" and "**/node_modules/**" both hide
// the directory and its subtree.
//
// A bare name with no metacharacters is just the degenerate case: it
// compiles to a segment-anchored match, so "node_modules" matches the
// segment node_modules anywhere in the path but not node_modules_old
// or my_node_modules.

export const DEFAULT_IGNORE = ["node_modules"];

export function isIgnored(path: string, patterns: string[]): boolean {
  if (patterns.length === 0) return false;
  // canonicalizePath strips the trailing slash and leaves a leading
  // "/" for non-root paths; matching happens in segment-relative
  // space (no leading slash) so a bare name anchors at segment
  // boundaries.
  const relative = path
    .split("/")
    .filter((s) => s.length > 0)
    .join("/");
  for (const pattern of patterns) {
    if (globMatcher(pattern).test(relative)) return true;
  }
  return false;
}

// Bound the compiled-pattern cache. Subscriber-supplied ignore lists are
// arbitrary, so a long-lived Durable Object that sees many distinct
// patterns must not grow this map without limit. The Map preserves
// insertion order, so evicting the first key drops the oldest entry.
const MATCHER_CACHE_LIMIT = 1024;
const matcherCache = new Map<string, RegExp>();

function globMatcher(pattern: string): RegExp {
  const cached = matcherCache.get(pattern);
  if (cached !== undefined) return cached;
  const compiled = compileGlob(pattern);
  if (matcherCache.size >= MATCHER_CACHE_LIMIT) {
    const oldest = matcherCache.keys().next().value;
    if (oldest !== undefined) matcherCache.delete(oldest);
  }
  matcherCache.set(pattern, compiled);
  return compiled;
}

// One token per glob construct, longest-first so "**/" wins over
// "**" wins over "*". The trailing alternative is the set of regex
// metacharacters a literal segment might contain, which we escape.
// Matching every construct in a single pass is deliberate: a chain
// of `.replace` calls would rescan the regex fragments it just
// emitted (the "[^/]*/)*" from a "**/" replacement still contains
// "*", which a later "*" pass would mangle).
const GLOB_TOKEN = /\*\*\/|\*\*|\*|\?|[\\^$.|+()[\]{}]/g;

// Compile a glob to a RegExp matched against the segment-relative
// path (e.g. "a/node_modules/b" for "/a/node_modules/b"). A trailing
// "(?:/.*)?" lets a pattern that names a directory also match its
// whole subtree.
function compileGlob(pattern: string): RegExp {
  let body = pattern;
  const anchored = body.startsWith("/");
  // Leading slash anchors the pattern at the root.
  if (anchored) body = body.slice(1);
  // A trailing "/**" means "this directory and everything beneath
  // it". The shared "(?:/.*)?" suffix already covers the subtree, so
  // strip the marker rather than require a trailing slash.
  body = body.replace(/\/\*\*$/, "");
  // Floating patterns (no leading slash, not already rooted at "**/")
  // match at any depth: prepend a segment-spanning "**/".
  if (!anchored && !body.startsWith("**/")) body = `**/${body}`;

  const source = body.replace(GLOB_TOKEN, (token) => {
    switch (token) {
      // "**/" spans zero or more complete leading segments.
      case "**/":
        return "(?:[^/]+/)*";
      // Trailing or bare "**" spans the rest, slashes included.
      case "**":
        return ".*";
      // Single "*" stays within one segment.
      case "*":
        return "[^/]*";
      case "?":
        return "[^/]";
      // A regex metacharacter in a literal segment; escape it.
      default:
        return `\\${token}`;
    }
  });

  return new RegExp(`^${source}(?:/.*)?$`);
}
