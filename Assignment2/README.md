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
Final experiments should be run on `Wikipedia-EN-20120601_ARTICLES.tar`, which is not committed due to size.

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

## Build Notes

Problem 2 was developed and tested using Hadoop in WSL/Ubuntu.

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

Run Problem 2(a):

```bash
hadoop jar problem_2/jars/document_frequency.jar DocumentFrequency \
  data/Wikipedia-50-ARTICLES \
  problem_2/results/output_problem_2a \
  data/stopwords.txt
```

Generate the top 100 DF terms:

```bash
sort -k2,2nr problem_2/results/output_problem_2a/part-r-00000 | head -100 > problem_2/results/top100_df.tsv
```

Run Problem 2(b):

```bash
hadoop jar problem_2/jars/tfidf_score.jar TfIdfScore \
  data/Wikipedia-50-ARTICLES \
  problem_2/results/output_problem_2b \
  data/stopwords.txt \
  problem_2/results/top100_df.tsv
```

## Outputs

Current committed Problem 2 artifacts include:

- `problem_2/results/df_values.tsv`
- `problem_2/results/sample_outputs/tfidf_scores.tsv`

Additional full-run outputs can be generated into:

- `problem_2/results/output_problem_2a_full/`
- `problem_2/results/output_problem_2b_full/`

## Notes

- Use the small dataset only for development/testing.
- Use the full Wikipedia dataset for final experiments and screenshots.
- Detailed step-by-step instructions for Problem 2 are in `problem_2/problem_2.txt`.
