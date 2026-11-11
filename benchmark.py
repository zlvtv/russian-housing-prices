import argparse
import hashlib
import json
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description="Measure a fixed nationwide aggregation")
    parser.add_argument("--deals", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Russian housing scale test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        data = spark.read.parquet(str(args.deals.resolve())).select(
            "region", "quarter", "cad_quarter", F.round(F.col("price_per_m2") * 100).cast("long").alias("kopecks"),
        )
        timings = []
        digests = set()
        warmup_seconds = None
        for trial in range(4):
            # A new QueryExecution forces a new shuffle rather than reusing the previous action.
            query = data.groupBy("region", "quarter", "cad_quarter").agg(
                F.count("*").alias("observations"), F.sum("kopecks").alias("total_kopecks")
            )
            start = perf_counter()
            rows = query.collect()
            elapsed = perf_counter() - start
            payload = sorted((r.region, str(r.quarter), r.cad_quarter, r.observations, r.total_kopecks) for r in rows)
            digests.add(hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest())
            if trial:
                timings.append(round(elapsed, 3))
            else:
                warmup_seconds = round(elapsed, 3)
        if len(digests) != 1:
            raise RuntimeError("Repeated aggregations did not return identical results")
        executor_info = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
        endpoints = sorted(f"{info.host()}:{info.port()}" for info in executor_info)
        result = {
            "master": spark.sparkContext.master,
            "registered_endpoints_including_driver": endpoints,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "input_partitions": data.rdd.getNumPartitions(),
            "observations": sum(r.observations for r in rows),
            "groups": len(rows),
            "runs_seconds": timings,
            "warmup_seconds": warmup_seconds,
            "fresh_aggregation_each_trial": True,
            "median_seconds": median(timings),
            "aggregation_sha256": digests.pop(),
        }
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
