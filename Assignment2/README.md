# Assignment 2

This repository contains the solutions, supporting files, and sample outputs for Assignment 2.

## Repository Layout

```text
Assignment2/
├── README.md
├── lib/
├── data/
├── problem_1/
└── problem_2/
```

## Problem 2 Overview

Problem 2 is implemented as a two-stage Hadoop MapReduce pipeline:

1. Problem 2(a): compute document frequency (DF) for each term.
2. Problem 2(b): use the top 100 DF terms to compute per-document TF-based scores.

The sample dataset `Wikipedia-50-ARTICLES.tar` is included for testing.
Final experiments should be run on the full dataset, which is not committed due to size.

## Problem 2 Files

- Source: `problem_2/src/`
- Jars: `problem_2/jars/`
- Results: `problem_2/results/`
- Instructions: `problem_2/problem_2.txt`

Key source files:

- `problem_2/src/DocumentFrequency.java`
- `problem_2/src/TfIdfScore.java`
- `problem_2/src/TextProcessingUtils.java`
- `problem_2/src/WholeFileInputFormat.java`
- `problem_2/src/WholeFileRecordReader.java`

Supporting files:

- `data/stopwords.txt`
- `lib/opennlp-tools-1.9.3.jar`

## Preprocessing Notes

The following preprocessing steps are applied before counting:

- lowercase tokens and strip leading/trailing apostrophes
- drop tokens containing digits
- drop tokens shorter than 3 characters
- remove stopwords (checked before and after Porter stemming)
- remove common markup artifacts: `apo`, `categori`, `extern`, `http`, `link`, `quot`, `refer`

These filters are applied in the **Mapper** stage before emitting terms.

## Screenshots

Store runtime/output screenshots under:

- `problem_2/results/screenshots/`

## Build Notes

Problem 2 was developed and tested using Hadoop in WSL/Ubuntu.
The jobs force conservative local-mode defaults in code:
`mapreduce.local.map.tasks.maximum=1`, `mapreduce.task.io.sort.mb=32`,
`mapreduce.map.java.opts=-Xmx256m`, `mapreduce.reduce.java.opts=-Xmx256m`.

Before compiling, set:

- `JAVA_HOME`
- `HADOOP_HOME`

Compile from the `Assignment2` root:

```bash
mkdir -p problem_2/build/classes problem_2/jars

javac -d problem_2/build/classes \
  -cp ".:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:lib/opennlp-tools-1.9.3.jar" \
  problem_2/src/DocumentFrequency.java \
  problem_2/src/TfIdfScore.java \
  problem_2/src/TextProcessingUtils.java \
  problem_2/src/WholeFileInputFormat.java \
  problem_2/src/WholeFileRecordReader.java
```

Create jars from `problem_2/build/classes`:

```bash
jar cfe ~/assignment2-problem2-jars/document_frequency.jar DocumentFrequency \
  DocumentFrequency*.class TextProcessingUtils.class WholeFileInputFormat.class WholeFileRecordReader.class

jar cfe ~/assignment2-problem2-jars/tfidf_score.jar TfIdfScore \
  TfIdfScore*.class TextProcessingUtils.class WholeFileInputFormat.class WholeFileRecordReader.class
```

Because jar creation on `/mnt/c/...` can fail under WSL, create the jars in Linux home first and then copy them into `problem_2/jars/`.

## Running Problem 2

Expose OpenNLP to Hadoop:

```bash
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/mnt/c/Users/manzi/Downloads/NoSQL-main/NoSQL-main/NoSQL/Assignment2/lib/opennlp-tools-1.9.3.jar
```

Extract the sample dataset:

```bash
tar -xf data/Wikipedia-50-ARTICLES.tar -C data/
```

Run Problem 2(a) (small dataset):

```bash
hadoop jar problem_2/jars/document_frequency.jar \
  data/Wikipedia-50-ARTICLES \
  problem_2/results/output_problem_2a \
  data/stopwords.txt
```

Generate the top 100 DF terms:

```bash
sort -k2,2nr problem_2/results/output_problem_2a/part-r-00000 | head -100 > problem_2/results/top100_df.tsv
```

Run Problem 2(b) (small dataset):

```bash
hadoop jar problem_2/jars/tfidf_score.jar \
  data/Wikipedia-50-ARTICLES \
  problem_2/results/output_problem_2b \
  data/stopwords.txt \
  problem_2/results/top100_df.tsv
```

## Outputs

Current committed Problem 2 artifacts include:

- `problem_2/results/df_values.tsv`
- `problem_2/results/top100_df.tsv`
- `problem_2/results/sample_outputs/tfidf_scores.tsv`
- `problem_2/results/tfidf_scores_full.tsv`

Additional full-run outputs can be generated into:

- `~/assignment2-problem2-full/output_problem_2a_full/`
- `~/assignment2-problem2-full/output_problem_2b_full/`

Only the final `part-r-00000` files are copied into the repo:

- `problem_2/results/df_values.tsv`
- `problem_2/results/top100_df.tsv`
- `problem_2/results/tfidf_scores_full.tsv`

## Measured Runtimes (Full Dataset)

Measured on WSL/Ubuntu with conservative local-mode settings and Linux-native input storage
(`~/assignment2-data`):

- Problem 2(a): `real 4m19.412s`
- Problem 2(b): `real 4m6.313s`

## Notes

- Use the small dataset only for development/testing.
- Use the full Wikipedia dataset for final experiments and screenshots.
- Detailed step-by-step instructions for Problem 2 are in `problem_2/problem_2.txt`.

## Optional Full Verification

If you want a full correctness check of every TF‑IDF score, use the verifier:
`problem_2/src/VerifyTfIdf.java`. Instructions are included in `problem_2/problem_2.txt`.
