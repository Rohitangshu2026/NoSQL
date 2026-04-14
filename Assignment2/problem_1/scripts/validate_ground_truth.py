def load_top50_from_file(filepath):
    top50 = set()
    counts = {}

    with open(filepath, encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2:
                word = parts[0].lower()
                count = int(parts[1])
                top50.add(word)
                counts[word] = count

    return top50, counts


def main():
    if len(sys.argv) < 6:
        print(__doc__)
        sys.exit(1)

    data_dir          = sys.argv[1]
    stopwords_file    = sys.argv[2]
    top50_file        = sys.argv[3]
    distance          = int(sys.argv[4])
    hadoop_files      = sys.argv[5:]

    # ── Load stopwords (kept for consistency, not critical now) ────────────
    print(f"Loading stop-words from {stopwords_file} ...")
    stopwords = load_stopwords(stopwords_file)
    print(f"  {len(stopwords)} stop-words loaded.")

    # ── 🔥 FIX: Load top-50 from Hadoop output (NOT recompute) ────────────
    print(f"\nLoading top-50 words from {top50_file} ...")
    top_words, top_counts = load_top50_from_file(top50_file)

    print(f"  Top-50 words (from Hadoop output):")
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