import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from time import perf_counter

import numpy as np
from pyspark.sql import SparkSession, functions as F


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def block_interval(groups, estimate):
    """Resample complete geographical groups. Report the limits of the middle 95%."""
    if len(groups) < 5:
        return None
    rng = np.random.default_rng(60202)
    estimates = []
    for sample in rng.integers(len(groups), size=(2_000, len(groups))):
        estimate_value = estimate(np.concatenate([groups[i] for i in sample], axis=0))
        if np.isfinite(estimate_value):
            estimates.append(estimate_value)
    return np.quantile(estimates, [0.025, 0.975]).round(4).tolist() if estimates else None


def summarize_panel(cells, totals):
    by_period = defaultdict(dict)
    for row in cells:
        key = (row.cad_district, row.locality, row.area_bin)
        by_period[row.region, row.quarter][key] = (row.observations, row.median_rub_m2)
    totals = {(row.region, row.quarter): (row.observations, row.median_rub_m2) for row in totals}
    output = []
    for (region, recent), now in sorted(by_period.items()):
        earlier = recent.replace(year=recent.year - 1)
        before = by_period.get((region, earlier))
        if not before:
            continue
        for threshold in (5, 10, 20):
            keys = sorted(
                key for key in before.keys() & now.keys()
                if min(before[key][0], now[key][0]) >= threshold
            )
            groups = defaultdict(list)
            for key in keys:
                groups[key[0]].append(100 * (now[key][1] / before[key][1] - 1))
            distributions = [np.array(values) for values in groups.values()]
            old_n, old_median = totals[region, earlier]
            new_n, new_median = totals[region, recent]
            base_counts = Counter()
            current_counts = Counter()
            for key, (n, _) in before.items():
                base_counts[key[0]] += n
            for key, (n, _) in now.items():
                current_counts[key[0]] += n
            old_named = sum(base_counts.values())
            new_named = sum(current_counts.values())
            shift = sum(abs(base_counts[k] / old_named - current_counts[k] / new_named)
                        for k in base_counts.keys() | current_counts.keys()) / 2
            output.append({
                "region": region,
                "earlier": earlier.isoformat(),
                "recent": recent.isoformat(),
                "minimum_in_both_periods": threshold,
                "raw_n_earlier": old_n,
                "raw_n_recent": new_n,
                "raw_change_pct": round(100 * (new_median / old_median - 1), 3),
                "locality_area_cells": len(keys),
                "cadastral_districts": len(groups),
                "coverage_earlier_pct": round(100 * sum(before[key][0] for key in keys) / old_n, 2),
                "coverage_recent_pct": round(100 * sum(now[key][0] for key in keys) / new_n, 2),
                "district_share_shift_pct": round(100 * shift, 2),
                "panel_change_pct": round(float(np.median(np.concatenate(distributions))), 3) if keys else None,
                "district_bootstrap_95_pct": block_interval(distributions, np.median),
            })
    return output


def rank(values):
    unique, inverse, counts = np.unique(values, return_inverse=True, return_counts=True)
    del unique
    return (np.cumsum(counts) - (counts + 1) / 2)[inverse]


def within_district_correlation(groups):
    # Ranks are centered and scaled within each district, so district price levels do not drive the result.
    return float(np.mean(groups[:, 0] * groups[:, 1]))


def housing_associations(rows):
    results = []
    for region in (47, 78):
        local = [row for row in rows if row["region"] == region]
        for field in ("median_building_year", "average_reported_deterioration"):
            districts = defaultdict(list)
            complete = [row for row in local if row[field] is not None]
            for row in complete:
                district = ":".join(row["cad_quarter"].split(":")[:2])
                districts[district].append((row[field], row["median_rub_m2"]))
            groups = []
            for values in districts.values():
                if len(values) < 5:
                    continue
                x, y = np.array(values).T
                x, y = rank(x), rank(y)
                if x.std() and y.std():
                    groups.append(np.column_stack(((x - x.mean()) / x.std(), (y - y.mean()) / y.std())))
            results.append({
                "region": region,
                "characteristic": field,
                "all_matched_blocks": len(local),
                "complete_blocks": len(complete),
                "analyzed_blocks": sum(len(group) for group in groups),
                "cadastral_districts": len(groups),
                "within_district_rank_correlation": (
                    round(within_district_correlation(np.concatenate(groups)), 4) if groups else None
                ),
                "district_bootstrap_95": block_interval(groups, within_district_correlation),
            })
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--deals", required=True, type=Path)
    parser.add_argument("--results", required=True, type=Path)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Matched Russian housing comparisons").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        started = perf_counter()
        country = spark.read.parquet(str(args.deals.resolve()))
        national = country.groupBy("quarter").agg(
            F.count("*").alias("observations"),
            F.percentile_approx("price_per_m2", 0.5, 20_000).alias("median_rub_m2"),
        ).orderBy("quarter").collect()
        dump(args.results / "16_national_quarterly.json", [
            {**row.asDict(), "quarter": row.quarter.isoformat()} for row in national
        ])
        data = country.where(F.col("region").isin(47, 78)).cache()
        median = F.percentile_approx("price_per_m2", 0.5, 20_000).alias("median_rub_m2")
        totals = data.groupBy("region", "quarter").agg(F.count("*").alias("observations"), median).collect()
        locality = F.regexp_replace(F.lower(F.trim("city")), r"\s+", " ")
        cells = (
            data.withColumn("cad_district", F.regexp_extract("cad_quarter", r"^([0-9]{2}:[0-9]{2}):", 1))
            .withColumn("locality", F.when(F.col("region") == 78, F.lit("")).otherwise(locality))
            .withColumn("area_bin", F.when(F.col("area") < 40, 0).when(F.col("area") < 65, 1).otherwise(2))
            .where((F.length("cad_district") > 0) & ((F.col("region") == 78) | (F.length("locality") > 0)))
            .groupBy("region", "quarter", "cad_district", "locality", "area_bin")
            .agg(F.count("*").alias("observations"), median)
            .collect()
        )
        dump(args.results / "11_matched_panel.json", summarize_panel(cells, totals))
        matched_seconds = perf_counter() - started
        started = perf_counter()
        housing = json.loads((args.results / "07_focus_blocks.json").read_text(encoding="utf-8"))
        dump(args.results / "12_housing_associations.json", housing_associations(housing))
        dump(args.results / "13_research_seconds.json", {
            "matched_panel": round(matched_seconds, 3),
            "housing_associations": round(perf_counter() - started, 3),
        })
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
