#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
raw="${1:-$PWD/data}"
output="${2:-$PWD/results}"
stage="$output/dkp.parquet"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYTHONDONTWRITEBYTECODE=1
export MPLCONFIGDIR="${MPLCONFIGDIR:-${TMPDIR:-/tmp}/russia-housing-mpl}"
submit="${SPARK_SUBMIT:-spark-submit}"

bash download.sh "$raw"
"$submit" --master 'local[6]' --driver-memory 3g --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=36 analyze.py --data "$raw" --output "$output" --stage-deals "$stage"
"$submit" --master 'local[6]' --driver-memory 2g --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=24 research.py --deals "$stage" --results "$output"
if [[ "${RUN_BENCHMARK:-0}" == 1 ]]; then
  for workers in 1 2; do
    "$submit" --master "local-cluster[$workers,2,1024]" --driver-memory 2g --executor-memory 1g --executor-cores 2 \
      --conf spark.ui.enabled=false --conf spark.sql.adaptive.enabled=false --conf spark.sql.shuffle.partitions=24 \
      --conf spark.sql.files.maxPartitionBytes=8388608 --conf spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=4 \
      --conf spark.executor.extraJavaOptions=-XX:ActiveProcessorCount=2 benchmark.py --deals "$stage" \
      --output "$output/14_benchmark_${workers}_worker.json"
  done
fi
"$PYSPARK_PYTHON" report.py --results "$output" --output "$output/view"
"$PYSPARK_PYTHON" verify.py --results "$output"
