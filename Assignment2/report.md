# Assignment 2 Report

## Problem 2: Indexing Documents via Hadoop

### Document Frequency Computation

**Mapper**  
Each Wikipedia article is treated as one document. The mapper tokenizes the document,
applies preprocessing (lowercasing, stopword filtering, stemming, etc.), and emits each
distinct term once per document.

Output format:
```
TERM<TAB>DF
```

**Reducer**  
The reducer sums counts to obtain the number of documents in which each term appears (DF).

**Pseudocode (Mapper/Reducer)**
```
Mapper(docId, docText):
    terms = empty set
    for token in tokenize(docText):
        t = preprocess(token)
        if t valid:
            terms.add(t)
    for t in terms:
        emit(t, 1)

Reducer(term, counts):
    DF = sum(counts)
    emit(term, DF)
```

### TF‑IDF Computation

**Setup**  
The top 100 DF terms from Part (a) are loaded from `top100_df.tsv` via Hadoop distributed
cache. Each term’s TF is combined with its DF to compute:

SCORE = TF × log(10000/DF + 1)

Output format:
```
ID<TAB>TERM<TAB>SCORE
```

**Mapper**  
For each document, the mapper builds a small in‑memory map (stripe) of term
frequencies (TF) for only those 100 terms, then emits the computed score.

**Reducer**  
Passthrough reducer (emits the mapper output).

**Pseudocode (Mapper/Reducer)**
```
Mapper(docId, docText):
    tf = empty map
    for token in tokenize(docText):
        t = preprocess(token)
        if t in top100:
            tf[t] += 1
    for each t in tf:
        score = tf[t] * log(10000/DF[t] + 1)
        emit(docId + "\t" + t, score)

Reducer(key, values):
    emit(key, value)   // passthrough
```

### Preprocessing Details
Applied in the mapper before emitting terms:
- lowercase tokens and strip leading/trailing apostrophes  
- drop tokens containing digits  
- drop tokens shorter than 3 characters  
- remove stopwords (checked before and after stemming)  
- remove markup artifacts: `apo`, `categori`, `extern`, `http`, `link`, `quot`, `refer`

### Runtime Analysis (Full Dataset)
Execution environment: WSL/Ubuntu, full dataset extracted into Linux‑native storage.

Measured runtimes:
- **Problem 2(a):** `real 4m19.412s`
- **Problem 2(b):** `real 4m6.313s`

### Screenshots
Here are the screenshots of successful full runs and runtimes:

#### Problem 2(a)
![Problem 2(a) runtime screenshot](problem_2/results/screenshots/NoSQL%20A2%20P2A.png)

#### Problem 2(b)
![Problem 2(b) runtime screenshot](problem_2/results/screenshots/NoSQL%20A2%20P2B.png)

### Additional Notes
- The full dataset `Wikipedia-EN-20120601_ARTICLES.tar.gz` was used for final experiments.
- Sample outputs and full result files are included in `problem_2/results/`.

### Verification (Optional)
An optional full‑correctness verifier is available in `problem_2/src/VerifyTfIdf.java`.
It recomputes every TF‑IDF score from the dataset and compares it against
`problem_2/results/tfidf_scores_full.tsv`. Compile/run commands are documented in
`problem_2/problem_2.txt`.

#### Verification Screenshot
![Problem 2(b) full verification screenshot](problem_2/results/screenshots/NoSQL%20A2%20P2B%20Validation.png)
