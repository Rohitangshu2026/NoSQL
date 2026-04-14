#!/bin/bash
# ---------------------------------------------------------------------------
# validate_all.sh  –  validate all co-occurrence outputs against ground truth
# Run from: problem_1/scripts/
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$PROJ_DIR/.." && pwd)"

DATA_DIR="$REPO_DIR/data/Wikipedia-EN/Wikipedia-EN-20120601_ARTICLES"
STOPWORDS="$REPO_DIR/data/stopwords.txt"
RESULTS_DIR="$PROJ_DIR/results"
VALIDATOR="$SCRIPT_DIR/validate_ground_truth.py"

if [[ ! -f "$VALIDATOR" ]]; then
    echo "ERROR: $VALIDATOR not found."
    exit 1
fi

if [[ ! -d "$RESULTS_DIR" ]]; then
    echo "ERROR: $RESULTS_DIR not found. Run run_all.sh first."
    exit 1
fi

OVERALL_PASS=0
OVERALL_FAIL=0

for D in 1 2 3 4; do
    echo ""
    echo "========================================="
    echo "   VALIDATING DISTANCE = $D"
    echo "========================================="

    FILES=()
    for f in "$RESULTS_DIR"/*_d${D}/part-r-00000; do
        [[ -f "$f" ]] && FILES+=("$f")
    done

    if [[ ${#FILES[@]} -eq 0 ]]; then
        echo "No output files found for distance $D — skipping."
        continue
    fi

    echo "Files to validate:"
    for f in "${FILES[@]}"; do
        echo "  $(basename "$(dirname "$f")")/$(basename "$f")"
    done
    echo ""

    TOP50_FILE="$RESULTS_DIR/top50words.txt"

    if python3 "$VALIDATOR" \
        "$DATA_DIR" \
        "$STOPWORDS" \
        "$TOP50_FILE" \
        "$D" \
        "${FILES[@]}"; then
        OVERALL_PASS=$(( OVERALL_PASS + 1 ))
    else
        OVERALL_FAIL=$(( OVERALL_FAIL + 1 ))
    fi

done

echo ""
echo "========================================="
echo "OVERALL: $OVERALL_PASS distance(s) fully passed, $OVERALL_FAIL failed"
echo "========================================="
[[ $OVERALL_FAIL -eq 0 ]]