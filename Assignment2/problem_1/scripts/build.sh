#!/bin/bash
# ---------------------------------------------------------------------------
# build.sh  –  compile all sources and package per-job jars
# Run from: problem_1/scripts/
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # problem_1/
SRC_DIR="$PROJ_DIR/src"
CLS_DIR="$PROJ_DIR/classes"
JAR_DIR="$PROJ_DIR/jars"
LIB_DIR="$(cd "$PROJ_DIR/../lib" && pwd)"

mkdir -p "$CLS_DIR" "$JAR_DIR"

HADOOP_CP="$(hadoop classpath)"
COMPILE_CP="$HADOOP_CP:$LIB_DIR/opennlp-tools-1.9.3.jar"

echo "[1/3] Compiling Java sources..."
javac -classpath "$COMPILE_CP" -d "$CLS_DIR" "$SRC_DIR"/*.java

echo "      Compilation successful."

# Helper: build a jar from an explicit list of class-name prefixes
# Usage: make_jar <jar-name> <ClassPrefix1> [ClassPrefix2 ...]
# The shared infrastructure classes (Custom*) are always included.
make_jar() {
    local jar_name="$1"; shift
    local patterns=("$@")

    # Always bundle shared infrastructure
    local shared_patterns=(
        "CustomCombineFileLineRecordReader"
        "CustomFileInputFormat"
        "CustomLineRecordReader"
    )

    # Build find expressions
    local find_args=()
    for pat in "${shared_patterns[@]}" "${patterns[@]}"; do
        find_args+=(-name "${pat}*.class" -o)
    done
    # Remove trailing -o
    unset 'find_args[${#find_args[@]}-1]'

    # Collect matching class files relative to CLS_DIR
    local class_files
    class_files=$(cd "$CLS_DIR" && find . \( "${find_args[@]}" \) | sed 's|^\./||')

    if [[ -z "$class_files" ]]; then
        echo "ERROR: No class files found for jar '$jar_name'"
        exit 1
    fi

    (cd "$CLS_DIR" && jar cf "$JAR_DIR/$jar_name.jar" $class_files)
    echo "      Created jars/$jar_name.jar"
}

# Package jars
echo "[2/3] Packaging jars..."

make_jar wordcount \
    "WordCount\$" \
    "WordCount2" \
    "TopFrequentWords"

# Co-occurrence jars
make_jar pairs          "PairsCoOccurrence"
make_jar stripes        "StripesCoOccurrence"
make_jar pairs_class    "PairsClassLevelAggregation"
make_jar pairs_func     "PairsFunctionLevelAggregation"
make_jar stripes_class  "StripesClassLevelAggregation"
make_jar stripes_func   "StripesFunctionLevelAggregation"

echo "[3/3] Done. Jars written to $JAR_DIR/"
ls -lh "$JAR_DIR/"