#!/usr/bin/env python3
"""
validate_ground_truth.py

Independently validates all Hadoop co-occurrence outputs against a
Python-computed ground truth.  Does NOT use top50words.txt — it
recomputes the top-50 frequent words itself from the raw corpus,
using the same stop-word list that WordCount2 uses, so the comparison
is fully self-contained and trustworthy.

Usage:
    python3 validate_ground_truth.py \
        <data_dir> <stopwords_file> <distance> \
        <hadoop_output_file1> [hadoop_output_file2] ...

The script auto-detects whether each output file is pairs or stripes
format from the filename, loads it, compares against the ground truth,
and prints a pass/fail summary.
"""

import os
import re
import sys
from collections import defaultdict


# ---------------------------------------------------------------------------
# Tokenization logic matching MapReduce Java mappers
# ---------------------------------------------------------------------------
html_entity_re = re.compile(r"&[a-z]+;")
html_tag_re = re.compile(r"<[^>]+>")
url_re = re.compile(r"(https?://|www\.)\S+")
wiki_link_re = re.compile(r"\[\[[^\]]+\]\]")
split_re = re.compile(r"[^a-zA-Z]+")

def tokenize(line):
    line = line.lower()
    line = html_entity_re.sub("", line)
    line = html_tag_re.sub("", line)
    line = url_re.sub("", line)
    line = wiki_link_re.sub("", line)
    
    tokens = []
    for token in split_re.split(line):
        if len(token) >= 2:
            tokens.append(token)
    return tokens


# ---------------------------------------------------------------------------
# Step 1 — recompute top-50 words from raw corpus
# ---------------------------------------------------------------------------

def load_stopwords(filepath):
    stopwords = set()
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            w = line.strip().lower()
            if w:
                stopwords.add(w)
    return stopwords


def compute_top50(data_dir, stopwords):
    """
    Uses exact tokenization as WordCount2 and Pairs/Stripes Mappers.
    """
    counts = defaultdict(int)

    for root, _, files in os.walk(data_dir):
        for fname in files:
            path = os.path.join(root, fname)
            try:
                with open(path, encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        for token in tokenize(line):
                            if token not in stopwords:
                                counts[token] += 1
            except OSError:
                continue

    top50 = sorted(counts, key=lambda w: (-counts[w], w))[:50]
    return set(top50), {w: counts[w] for w in top50}




# ---------------------------------------------------------------------------
# Step 2 — compute ground-truth co-occurrence counts
# ---------------------------------------------------------------------------

def compute_ground_truth(data_dir, top_words, distance):
    """
    For each algorithm we replicate the exact tokenisation used in the
    corresponding Java mapper so the comparison is apples-to-apples.

    Distance windows are computed over ALL valid tokens (not just top-50).
    The top-50 filter only determines what gets emitted as pairs.
    """
    cooccur = defaultdict(int)          # (center, neighbor) -> count

    for root, _, files in os.walk(data_dir):
        for fname in files:
            path = os.path.join(root, fname)
            try:
                with open(path, encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        # Build dense list of ALL valid tokens (matching Java)
                        valid_words = tokenize(line)
                        n = len(valid_words)
                        for i in range(n):
                            center = valid_words[i]
                            if center not in top_words:
                                continue
                            lo = max(0, i - distance)
                            hi = min(n - 1, i + distance)
                            for j in range(lo, hi + 1):
                                if j == i:
                                    continue
                                neighbor = valid_words[j]
                                if neighbor not in top_words:
                                    continue
                                cooccur[(center, neighbor)] += 1
            except OSError:
                continue

    return cooccur


# ---------------------------------------------------------------------------
# Step 3 — load Hadoop output
# ---------------------------------------------------------------------------

def load_hadoop_output(filepath):
    """
    Handles three output formats written by the Java jobs:

    Pairs baseline / class / func:
        (word1,word2)   <TAB>   count

    Stripes baseline / class / func:
        word   <TAB>   {neighbor1:count1, neighbor2:count2, ...}

    Returns dict  (center, neighbor) -> count
    """
    result = {}
    stripes_inner_re = re.compile(r",\s*")

    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if "\t" not in line:
                continue
            key, val = line.split("\t", 1)
            key = key.strip()
            val = val.strip()

            if val.startswith("{") and val.endswith("}"):
                # ── Stripes format ──────────────────────────────────────
                center = key
                inner = val[1:-1]
                if not inner:
                    continue
                for item in stripes_inner_re.split(inner):
                    if ":" not in item:
                        continue
                    neighbor, cnt = item.rsplit(":", 1)
                    try:
                        result[(center.strip(), neighbor.strip())] = int(cnt.strip())
                    except ValueError:
                        pass

            elif key.startswith("(") and key.endswith(")"):
                # ── Pairs format  (center,neighbor) ────────────────────
                inner = key[1:-1]
                if "," not in inner:
                    continue
                # Split on first comma only — words themselves have no commas
                center, neighbor = inner.split(",", 1)
                try:
                    result[(center.strip(), neighbor.strip())] = int(val)
                except ValueError:
                    pass

    return result


# ---------------------------------------------------------------------------
# Step 4 — compare
# ---------------------------------------------------------------------------

def validate_top50(filepath, gt_counts):
    print(f"\n--- Top-50 Words Validation: {os.path.basename(filepath)} ---")
    if not os.path.exists(filepath):
        print("  SKIP: file not found.")
        return 0, 1

    hadoop_counts = {}
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    hadoop_counts[parts[0]] = int(parts[1])
                except ValueError:
                    pass

    errors = 0
    for w, c in gt_counts.items():
        if w not in hadoop_counts:
            print(f"  MISSING '{w}'  gt={c}")
            errors += 1
        elif hadoop_counts[w] != c:
            print(f"  MISMATCH '{w}'  gt={c}  hadoop={hadoop_counts[w]}")
            errors += 1
            
    for w, c in hadoop_counts.items():
        if w not in gt_counts:
            print(f"  EXTRA '{w}'  hadoop={c}")
            errors += 1

    if errors == 0:
        print(f"  PASS ({len(gt_counts)} words matched perfectly)")
        return 1, 0
    else:
        print(f"  FAIL {errors} discrepancies in top-50 words")
        return 0, 1

def validate_one(filepath, ground_truth, label):
    print(f"\n--- {label}: {os.path.basename(filepath)} ---")

    if not os.path.exists(filepath):
        print(f"  SKIP: file not found.")
        return 0, 1   # (passed, failed)

    hadoop = load_hadoop_output(filepath)

    errors = 0
    # Ground truth pairs missing or wrong in Hadoop output
    for pair, gt_count in ground_truth.items():
        h_count = hadoop.get(pair)
        if h_count is None:
            print(f"  MISSING  ({pair[0]}, {pair[1]})  gt={gt_count}")
            errors += 1
        elif h_count != gt_count:
            print(f"  MISMATCH ({pair[0]}, {pair[1]})  gt={gt_count}  hadoop={h_count}")
            errors += 1

    # Hadoop pairs not in ground truth
    for pair, h_count in hadoop.items():
        if pair not in ground_truth:
            print(f"  EXTRA    ({pair[0]}, {pair[1]})  hadoop={h_count}")
            errors += 1

    if errors == 0:
        print(f"  PASS  ({len(ground_truth)} pairs matched perfectly)")
        return 1, 0
    else:
        print(f"  FAIL  {errors} discrepancies  "
              f"(gt={len(ground_truth)} pairs, hadoop={len(hadoop)} pairs)")
        return 0, 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 6:
        print(__doc__)
        sys.exit(1)

    data_dir          = sys.argv[1]
    stopwords_file    = sys.argv[2]
    top50_file        = sys.argv[3]
    distance          = int(sys.argv[4])
    hadoop_files      = sys.argv[5:]

    # ── Recompute top-50 independently ────────────────────────────────────
    print(f"Loading stop-words from {stopwords_file} ...")
    stopwords = load_stopwords(stopwords_file)
    print(f"  {len(stopwords)} stop-words loaded.")

    print(f"\nScanning corpus to recompute top-50 frequent words ...")
    top_words, top_counts = compute_top50(data_dir, stopwords)
    print(f"  Top-50 words (independently computed):")
    for w in sorted(top_counts, key=lambda x: -top_counts[x]):
        print(f"    {w:20s}  {top_counts[w]}")

    # ── Compute ground-truth co-occurrence ────────────────────────────────
    print(f"\nComputing ground-truth co-occurrence (distance={distance}) ...")
    ground_truth = compute_ground_truth(data_dir, top_words, distance)
    print(f"  {len(ground_truth)} unique (center, neighbor) pairs in ground truth.")

    passed = failed = 0

    # ── Validate Top50 ────────────────────────────────────────────────────
    p, f = validate_top50(top50_file, top_counts)
    passed += p
    failed += f

    # ── Validate each Hadoop output file ──────────────────────────────────
    for fpath in hadoop_files:
        fname = os.path.basename(fpath).lower()
        parent = os.path.basename(os.path.dirname(fpath)).lower()

        # Detect algorithm from parent directory name
        if "stripes" in parent:
            label = "Stripes"
        elif "pairs" in parent:
            label = "Pairs"
        else:
            label = "Unknown"

        p, f = validate_one(fpath, ground_truth, label)
        passed += p
        failed += f

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"SUMMARY  distance={distance}:  "
          f"{passed} passed,  {failed} failed  "
          f"(of {passed+failed} files checked)")
    print(f"{'='*50}")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()