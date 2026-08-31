---
name: like-similar-to-implementation
description: DONE — LIKE/ILIKE/SIMILAR TO + REGEXP_* pattern matching shipped to main; key design decisions
metadata: 
  node_type: memory
  type: project
  originSessionId: 100c1d61-19f5-491a-b71e-0bc630edf74a
---

DONE (committed/pushed to main, fde5bcb): `LIKE` / `ILIKE` / `SIMILAR TO` with optional `ESCAPE`.

Design decisions worth remembering (not obvious from a glance at the code):
- **Parser-sugar pattern, not new AST nodes.** Parse-time desugar to boolean scalar-function calls `like` / `ilike` / `similar_to` (3rd arg = escape), resolved + lowered via [[scalar-function-registry-temporal]]. `[NOT]` wraps in `UnaryExpression(Not, …)` so 3VL NULL handling is inherited (same trick as line ~2287 in Resolver). Mirrors how `||`, POSITION-IN, BETWEEN are handled.
- **Contextual keywords** (`IsContextualKeyword`/new `IsContextualKeywordAt`), NOT reserved — `like`/`ilike`/`similar`/`to`/`escape` stay usable as identifiers. Deliberate, to avoid breaking existing queries; verified by a test.
- **Default escape = backslash** (PostgreSQL-aligned, since the codebase is PG-aligned on NULL semantics); `ESCAPE ''` disables it. The lexer is standard-conforming (no backslash processing in string literals), so `\` reaches the pattern intact. **This is the most likely thing a user might want flipped** (SQL Server / standard use no default escape).
- **Lowering = translate to `System.Text.RegularExpressions.Regex`**, whole-string anchored `\A(?:…)\z`, `RegexOptions.Singleline` so `_`/`%` span newlines. Constant pattern → compiled once at build time and baked as a `Constant(Regex)`; dynamic pattern → translated + cached (ConcurrentDictionary, pure memoization). No typed fast path (BuildTyped returns null → structural compile), like the other string predicates. All in `BuiltinScalarFunctions.cs` (`SqlPatternMatch` class + `RegexMatch`/`PatternMatch`/`PatternMatchEsc` runtime helpers).
- **SIMILAR TO** passes SQL-regex metacharacters `| * + ? ( ) { } [ ]` through; `.` `^` `$` etc. are literals.

Deferred: POSIX class names inside SIMILAR TO brackets (`[[:alpha:]]`) — .NET regex doesn't support that syntax.

---

**REGEXP family** (committed/pushed to main, fbb3b74): `REGEXP_LIKE` / `REGEXP_REPLACE` / `REGEXP_SUBSTR`. Natural follow-on reusing the same `SqlPatternMatch` regex cache.
- **Substring matches, NOT anchored** (the key difference from LIKE) — pattern handed straight to .NET `Regex` (`SqlPatternMatch.CompileRegex` + `PosixCache`); POSIX ERE ≈ a subset of .NET syntax. So no `\A…\z` wrapping and no Singleline by default.
- Optional trailing **flags** string (`SqlPatternMatch.ParseFlags`): `i` ignore-case, `c` case-sensitive (default; clears prior `i`), `m` multiline, `s` dot-matches-newline, `g` global (REGEXP_REPLACE only). Unknown flag → throws.
- **REGEXP_REPLACE is PG-aligned**: replaces FIRST match by default, ALL with `g`. Replacement backrefs `\1`..`\9` / `\&` are translated to .NET `$1`/`$0` by `SqlPatternMatch.TranslateReplacement` (literal `$`→`$$`).
- REGEXP_LIKE → BOOLEAN (constant pattern precompiled via `TryConstRegex`); REGEXP_SUBSTR → first match or NULL. All NULL-propagating, no typed fast path.
- **Deferred:** the `~` / `~*` / `!~` / `!~*` PG match operators (need new lexer tokens around `!`/`!=` — that's why they were split out). Shares the `[[:alpha:]]` POSIX-class gap.
