#!/bin/bash
# ---------------------------------------------------------------------------
# run_all.sh  –  end-to-end pipeline for Assignment II Problem 1
#
# Job sequence
#   1a. WordCount2          →  word frequencies (stop-words excluded)
#   1a. TopFrequentWords    →  top-50 words (MapReduce)
#   1b. PairsCoOccurrence   )
#   1c. StripesCoOccurrence )  for d in {1,2,3,4}
#   1e. Pairs{Class,Func}   )
#   1e. Stripes{Class,Func} )
#
# Run from: problem_1/scripts/
# ---------------------------------------------------------------------------
set -euo pipefail

# ── Resolve directories ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # problem_1/
REPO_DIR="$(cd "$PROJ_DIR/.." && pwd)"            # repo root
JAR_DIR="$PROJ_DIR/jars"
RESULTS_DIR="$PROJ_DIR/results"
DATA_DIR="$REPO_DIR/data"

mkdir -p "$RESULTS_DIR"

# ── Inputs ─────────────────────────────────────────────────────────────────
INPUT="$DATA_DIR/Wikipedia-EN/Wikipedia-EN-20120601_ARTICLES"
STOPWORDS="$DATA_DIR/stopwords.txt"

# Outputs
WC_OUT="$RESULTS_DIR/wordcount"
TOP_OUT="$RESULTS_DIR/top50"
TOP_WORDS_FILE="$RESULTS_DIR/top50words.txt"

LOGFILE="$RESULTS_DIR/runtime_log.txt"

SPLIT_MAX=52428800
SPLIT_MIN=26214400

for f in "$INPUT" "$STOPWORDS" \
          "$JAR_DIR/wordcount.jar" \
          "$JAR_DIR/pairs.jar" \
          "$JAR_DIR/stripes.jar" \
          "$JAR_DIR/pairs_class.jar" \
          "$JAR_DIR/pairs_func.jar" \
          "$JAR_DIR/stripes_class.jar" \
          "$JAR_DIR/stripes_func.jar"; do
    if [[ ! -e "$f" ]]; then
        echo "✗ ERROR: required path not found: $f"
        echo "         Run build.sh first, and verify paths."
        exit 1
    fi
done


log_section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "=== $1 ===" >> "$LOGFILE"
}

timed_run() {
    local label="$1"; shift

    echo ""
    echo "┌─────────────────────────────────────────────────"
    echo "│  $label"
    echo "└─────────────────────────────────────────────────"
    echo ">> $label" >> "$LOGFILE"

    local start end elapsed
    start=$(python3 -c "import time; print(int(time.time()*1000))")

    # All Hadoop logs go to LOGFILE only — terminal stays clean
    "$@" >> "$LOGFILE" 2>&1
    local rc=$?

    end=$(python3 -c "import time; print(int(time.time()*1000))")
    elapsed=$(( end - start ))
    local secs=$(( elapsed / 1000 ))
    local ms=$(( elapsed % 1000 ))

    if [[ $rc -ne 0 ]]; then
        printf "  FAILED after %d.%03ds — check %s\n" "$secs" "$ms" "$LOGFILE"
        echo "ERROR: '$label' failed (exit $rc)." >> "$LOGFILE"
        exit $rc
    fi

    printf "    Done in %d.%03ds\n" "$secs" "$ms"
    printf "   elapsed: %d.%03ds\n" "$secs" "$ms" >> "$LOGFILE"
}

safe_rm_rf() {
    [[ -d "$1" ]] && rm -rf "$1"
    return 0
}

resolve_cache_uri() {
    local local_path="$1"
    local default_fs
    default_fs="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo 'file:///')"

    if [[ "$default_fs" == hdfs://* ]]; then
        local fname
        fname="$(basename "$local_path")"
        echo "  Staging $fname to HDFS /tmp/ ..." >&2
        hdfs dfs -put -f "$local_path" "/tmp/$fname"
        echo "${default_fs%/}/tmp/$fname"
    else
        echo "$local_path"
    fi
}

{
    echo "=== Runtime Log ==="
    echo "Started  : $(date)"
    echo "Input    : $INPUT"
    echo "Stopwords: $STOPWORDS"
    echo "Split max: $SPLIT_MAX bytes"
    echo "Split min: $SPLIT_MIN bytes"
    echo ""
} > "$LOGFILE"

echo ""
echo "  Assignment II — Problem 1 Pipeline"
echo "  Input    : $INPUT"
echo "  Results  : $RESULTS_DIR"
echo "  Log      : $LOGFILE"

# Step 1a-i: WordCount2
log_section "1a-i: WordCount2 (stop-word filtered)"
safe_rm_rf "$WC_OUT"

timed_run "WordCount2" \
    hadoop jar "$JAR_DIR/wordcount.jar" \
        WordCount2 \
        -Dmapreduce.input.fileinputformat.split.maxsize="$SPLIT_MAX" \
        -Dmapreduce.input.fileinputformat.split.minsize="$SPLIT_MIN" \
        "$INPUT" "$WC_OUT" \
        -skippatterns "$STOPWORDS"

# Step 1a-ii: TopFrequentWords
log_section "1a-ii: TopFrequentWords (top 50)"
safe_rm_rf "$TOP_OUT"

timed_run "TopFrequentWords" \
    hadoop jar "$JAR_DIR/wordcount.jar" \
        TopFrequentWords \
        "$WC_OUT" "$TOP_OUT"

echo ""
echo "  Merging top-50 output ..."
hadoop fs -getmerge "$TOP_OUT" "$TOP_WORDS_FILE" >> "$LOGFILE" 2>&1

if [[ ! -s "$TOP_WORDS_FILE" ]]; then
    echo "  ✗ ERROR: top50words.txt is empty."
    exit 1
fi

echo "  ✓ Top-50 words:"
cat "$TOP_WORDS_FILE" | awk '{printf "    %-20s %s\n", $1, $2}'
cat "$TOP_WORDS_FILE" >> "$LOGFILE"

TOPWORDS_CACHE_URI="$(resolve_cache_uri "$TOP_WORDS_FILE")"
echo ""
echo "  Cache URI: $TOPWORDS_CACHE_URI"


# Steps 1b, 1c, 1e: Co-occurrence for d = {1, 2, 3, 4}
for D in 1 2 3 4; do

    log_section "Distance d=$D"

    # Pairs
    safe_rm_rf "$RESULTS_DIR/pairs_d$D"
    timed_run "Pairs (1b) d=$D" \
        hadoop jar "$JAR_DIR/pairs.jar" \
            PairsCoOccurrence \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/pairs_d$D"

    # Stripes
    safe_rm_rf "$RESULTS_DIR/stripes_d$D"
    timed_run "Stripes (1c) d=$D" \
        hadoop jar "$JAR_DIR/stripes.jar" \
            StripesCoOccurrence \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/stripes_d$D"

    # Pairs class-level
    safe_rm_rf "$RESULTS_DIR/pairs_class_d$D"
    timed_run "Pairs class-level (1e) d=$D" \
        hadoop jar "$JAR_DIR/pairs_class.jar" \
            PairsClassLevelAggregation \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/pairs_class_d$D"

    # Pairs function-level
    safe_rm_rf "$RESULTS_DIR/pairs_func_d$D"
    timed_run "Pairs func-level (1e) d=$D" \
        hadoop jar "$JAR_DIR/pairs_func.jar" \
            PairsFunctionLevelAggregation \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/pairs_func_d$D"

    # Stripes class-level
    safe_rm_rf "$RESULTS_DIR/stripes_class_d$D"
    timed_run "Stripes class-level (1e) d=$D" \
        hadoop jar "$JAR_DIR/stripes_class.jar" \
            StripesClassLevelAggregation \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/stripes_class_d$D"

    # Stripes function-level
    safe_rm_rf "$RESULTS_DIR/stripes_func_d$D"
    timed_run "Stripes func-level (1e) d=$D" \
        hadoop jar "$JAR_DIR/stripes_func.jar" \
            StripesFunctionLevelAggregation \
            -Dcooccur.distance="$D" \
            -topwords "$TOPWORDS_CACHE_URI" \
            "$INPUT" "$RESULTS_DIR/stripes_func_d$D"

done

# Summary
log_section "ALL RUNS COMPLETE"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Runtime Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

grep -E "^(>>|   elapsed)" "$LOGFILE" | paste - - \
    | awk '{
        label = $2
        for (i = 3; i <= NF-1; i++) label = label " " $i
        printf "  %-45s  %s\n", label, $NF
    }'

{
    echo ""
    echo "Finished: $(date)"
    echo ""
    echo "Runtime summary:"
    grep -E "^(>>|   elapsed)" "$LOGFILE" | paste - - \
        | awk '{
            label = $2
            for (i = 3; i <= NF-1; i++) label = label " " $i
            printf "  %-45s  %s\n", label, $NF
        }'
} >> "$LOGFILE"

echo ""
echo "  Full log : $LOGFILE"
echo "  Results  : $RESULTS_DIR/"