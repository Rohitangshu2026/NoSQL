#!/bin/bash
set -e

hdfs dfs -mkdir -p /nasa/logs/
echo "Please upload your log files to /nasa/logs/ using:"
echo "hdfs dfs -put <logfile> /nasa/logs/"
