#!/usr/bin/env bash
#
# compare-nexmark.sh — run the Nexmark throughput benchmark on both DbspNet and
# Feldera's own Rust DBSP engine, on this host, and print a merged events/s table.
#
# Both sides run the same query set over in-process-generated Nexmark events
# (the standard 1:3:46 Person/Auction/Bid stream), cold (every event new), with
# matched event count, micro-batch size, and core count. Only the queries
# DbspNet implements are compared: q0 q1 q2 q3 q4 q9.
#
# Usage:
#   scripts/compare-nexmark.sh [--events=N] [--cores=C] [--batch=B] [--runs=R]
#                              [--generators=G] [--queries="q0 q1 ..."]
#                              [--feldera=PATH] [--dbsp-only]
#
# Env overrides (same names, upper-case): EVENTS, CORES, BATCH, RUNS, GENERATORS,
#   QUERIES, FELDERA_DIR
#
# IMPORTANT — the two harnesses do NOT measure the same thing, so read the ratio
# with care:
#   * DbspNet pre-generates the whole event stream, then times push + Step() only.
#   * Feldera's bench generates events *inside* the timed window (a streaming
#     pipeline), so its elapsed includes event generation overlapped with compute.
#     With too few generator threads it becomes generation-bound and understates
#     the engine — hence --generators defaults to the core count here. It is still
#     a pipeline number: fairest for compute-heavy queries (q4, q9); for light
#     queries (q0-q2) Feldera's figure still carries generation overhead.
#   * Small --events understates Feldera (per-query DBSP runtime setup/teardown is
#     amortized over the stream; Feldera's published runs use 100M events). Raise
#     --events for a steadier read, and cross-check Feldera's published numbers.
#
# Other notes:
#   * The first Feldera run compiles the DBSP workspace in release — minutes.
#   * DbspNet reports the median of RUNS runs (after one warmup); Feldera's bench
#     runs each query once. For a tighter read, bump --runs and re-run a few times.
#   * events/s divides by the *whole* generated stream on both sides, so a query
#     that reads a subset of tables (e.g. q3 skips the 92% bids) reads high on
#     both — it is keeping up with that stream rate, not doing that much per row.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Host core count (Linux: nproc, macOS: sysctl).
if command -v nproc >/dev/null 2>&1; then HOSTCORES="$(nproc)"; else HOSTCORES="$(sysctl -n hw.ncpu)"; fi

EVENTS="${EVENTS:-1000000}"
CORES="${CORES:-$HOSTCORES}"
BATCH="${BATCH:-10000}"
RUNS="${RUNS:-3}"
GENERATORS="${GENERATORS:-$CORES}"
QUERIES="${QUERIES:-q0 q1 q2 q3 q4 q9}"
FELDERA_DIR="${FELDERA_DIR:-/Users/curt/Documents/GitHub/feldera}"
DBSP_ONLY=0

for a in "$@"; do
  case "$a" in
    --events=*)      EVENTS="${a#*=}" ;;
    --cores=*)       CORES="${a#*=}" ;;
    --batch=*)       BATCH="${a#*=}" ;;
    --runs=*)        RUNS="${a#*=}" ;;
    --generators=*)  GENERATORS="${a#*=}" ;;
    --queries=*)     QUERIES="${a#*=}" ;;
    --feldera=*)     FELDERA_DIR="${a#*=}" ;;
    --dbsp-only)     DBSP_ONLY=1 ;;
    -h|--help)       grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $a (try --help)" >&2; exit 2 ;;
  esac
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
DBSP_OUT="$WORK/dbsp.txt"
FELDERA_CSV="$WORK/feldera.csv"

echo "== Nexmark comparison =="
echo "   events=$EVENTS  cores=$CORES  batch=$BATCH  runs=$RUNS  feldera-generators=$GENERATORS"
echo "   queries=$QUERIES"
echo "   host: $(uname -mrs)"
echo

# ---- 1. DbspNet ----------------------------------------------------------------
echo "-- DbspNet (dotnet, W=1 and W=$CORES) ----------------------------------------"
dotnet run -c Release --project "$REPO/src/DbspNet.Benchmarks" -- \
  nexmark "$EVENTS" "$BATCH" "$RUNS" "$CORES" | tee "$DBSP_OUT"
echo

# ---- 2. Feldera ----------------------------------------------------------------
if [ "$DBSP_ONLY" -eq 0 ] && [ ! -d "$FELDERA_DIR" ]; then
  echo "!! Feldera repo not found at: $FELDERA_DIR"
  echo "   pass --feldera=PATH or --dbsp-only; showing DbspNet numbers only."
  DBSP_ONLY=1
fi

if [ "$DBSP_ONLY" -eq 0 ]; then
  echo "-- Feldera (cargo bench dbsp_nexmark, --cpu-cores $CORES) ---------------------"
  echo "   (first build compiles the DBSP workspace in release — may take minutes)"
  rm -f "$FELDERA_CSV"
  qargs=()
  for q in $QUERIES; do qargs+=(--query "$q"); done
  ( cd "$FELDERA_DIR" && cargo bench -p dbsp_nexmark --bench nexmark -- \
      --max-events="$EVENTS" \
      --cpu-cores "$CORES" \
      --input-batch-size "$BATCH" \
      --num-event-generators "$GENERATORS" \
      "${qargs[@]}" \
      --csv "$FELDERA_CSV" \
      --no-progress )
  echo
fi

# ---- 3. Merge + print ----------------------------------------------------------
python3 - "$DBSP_OUT" "$FELDERA_CSV" "$CORES" "$EVENTS" "$BATCH" "$QUERIES" <<'PY'
import csv, re, sys, os

dbsp_out, feldera_csv, cores, events, batch, queries = sys.argv[1:7]
cores = int(cores)
order = queries.split()

def num(s): return int(s.replace(",", ""))

# DbspNet console lines:
#   q0: W1=     538,262  W2=   1,422,046 events/s    2.64×  ok
#   q4:      238,690 events/s  (single-only)
#   q0:      538,262 events/s   lastΔ=9,200        (W=1 mode)
#   q5: DID NOT COMPILE — ...
dbsp = {}  # qid -> (w1, wn)  wn=None when single-only / W=1 / n/a
re_par   = re.compile(r'^\s*(q\d+):\s*W1=\s*([\d,]+)\s+W\d+=\s*([\d,]+)\s+events/s')
re_solo  = re.compile(r'^\s*(q\d+):\s*([\d,]+)\s+events/s\s*\(single-only\)')
re_one   = re.compile(r'^\s*(q\d+):\s*([\d,]+)\s+events/s')
re_fail  = re.compile(r'^\s*(q\d+):\s*DID NOT COMPILE')
for line in open(dbsp_out, encoding="utf-8", errors="replace"):
    if (m := re_par.match(line)):  dbsp[m[1]] = (num(m[2]), num(m[3]));            continue
    if (m := re_solo.match(line)): dbsp[m[1]] = (num(m[2]), None);                 continue
    if (m := re_fail.match(line)): dbsp[m[1]] = (None, None);                      continue
    if (m := re_one.match(line)):  dbsp[m[1]] = (num(m[2]), None);                 continue

# Feldera CSV: name,num_cores,num_events,elapsed(secs),...
feld = {}  # qid -> events/s
if feldera_csv and os.path.isfile(feldera_csv):
    with open(feldera_csv, newline="") as f:
        for row in csv.DictReader(f):
            try:
                eps = float(row["num_events"]) / float(row["elapsed"])
            except (KeyError, ValueError, ZeroDivisionError):
                continue
            feld[row["name"].strip().lower()] = eps

have_feld = bool(feld)
def fmt(v): return "n/a" if v is None else f"{round(v):,}"

print(f"== Merged: events/s (cold stream, {events} events, batch {batch}, {cores} cores) ==\n")
if have_feld:
    hdr = ["Query", "DbspNet W=1", f"DbspNet W={cores}", f"Feldera ({cores}c)", f"W={cores} / Feldera"]
else:
    hdr = ["Query", "DbspNet W=1", f"DbspNet W={cores}"]

rows = []
for q in order:
    w1, wn = dbsp.get(q, (None, None))
    cells = [q, fmt(w1), fmt(wn)]
    if have_feld:
        fe = feld.get(q)
        cells.append(fmt(fe))
        cells.append(f"{wn/fe:.2f}x" if (wn and fe) else "n/a")
    rows.append(cells)

widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(hdr)]
def line(cells): return "  ".join(c.rjust(widths[i]) if i else c.ljust(widths[i]) for i, c in enumerate(cells))
print(line(hdr))
print("  ".join("-"*w for w in widths))
for r in rows: print(line(r))

if not have_feld:
    print("\n(Feldera side skipped — --dbsp-only or repo not found.)")
else:
    print(f"\nW={cores}/Feldera = DbspNet W={cores} ÷ Feldera at {cores} cores (>1.0x = DbspNet faster).")
    print("\nCAVEAT — different harnesses, read with care:")
    print("  * DbspNet times push+Step() over a PRE-GENERATED stream; Feldera times")
    print("    event generation + compute together (a streaming pipeline).")
    print("  * So the ratio is fairest for compute-heavy queries (q4, q9). For light")
    print("    queries (q0-q2) Feldera's figure still carries event-generation cost and")
    print(f"    understates its engine. Small --events ({events}) also understates Feldera")
    print("    (per-query DBSP setup isn't amortized); raise --events and cross-check")
    print("    Feldera's published Nexmark numbers for an absolute read.")
PY
