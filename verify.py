import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Check release totals and consistency between result tables")
    parser.add_argument("--results", type=Path, default=Path(__file__).resolve().parent / "results")
    args = parser.parse_args()
    load = lambda name: json.loads((args.results / f"{name}.json").read_text(encoding="utf-8"))
    quality = load("01_quality")
    assert sum(row["all_rows"] for row in quality) == 6_756_046
    assert sum(row["usable_dkp_rows"] for row in quality) == 1_952_424
    assert sum(row["rows"] for row in load("05_housing_coverage")) == 956_147
    assert sum(row["count"] for row in load("08_cian_coverage")) == 456_182
    assert sum(row["bytes"] for row in load("inputs")) == 2_193_044_463

    cities = load("03_northwest_cities")
    keys = [(r["region"], r["cad_district"], r["city_key"], r["quarter"]) for r in cities]
    assert len(keys) == len(set(keys))
    assert all(row["observations"] >= 30 for row in cities)
    arkhangelsk = [r for r in cities if r["region"] == 29 and r["city_key"] == "архангельск"
                   and r["quarter"] == "2026-01-01"]
    assert len(arkhangelsk) == 1 and arkhangelsk[0]["observations"] == 916

    regional = {(r["region"], r["quarter"]): r for r in load("02_quarterly_regions")}
    sensitivity = load("17_filter_sensitivity")
    scenarios = {}
    for row in sensitivity:
        scenarios.setdefault((row["region"], row["quarter"]), {})[row["scenario"]] = row
    for key, rows in scenarios.items():
        assert rows["narrow"]["observations"] <= rows["medium"]["observations"] <= rows["wide"]["observations"]
        assert rows["wide"]["observations"] == regional[key]["observations"]
        # Independent aggregate queries can choose a slightly different approximate quantile.
        assert abs(rows["wide"]["median_rub_m2"] - regional[key]["median_rub_m2"]) <= 5
    assert all(0 <= row["tukey_flagged"] <= row["observations"]
               for row in load("19_statistical_price_flags"))

    sources = load("18_city_source_comparison")
    assert len(sources) == 32
    assert sum(row["relative_difference_pct"] is not None for row in sources) == 30
    assert all(row["observations"] is None for row in sources if row["city_name"] == "Moscow")
    spb = [row for row in sources if row["city_name"] == "Saint Petersburg"]
    for row in load("09_spb_offer_vs_contract"):
        counterpart = next(x for x in spb if x["year"] == row["year"])
        assert row["observations"] == counterpart["observations"]
        assert row["median_rub_m2"] == counterpart["median_rub_m2"]
        assert row["median_of_cluster_medians"] == counterpart["median_of_cluster_medians"]
    geography = load("20_geography_summary")
    assert len(geography["northwest_regions"]) == 11
    assert geography["national"]["observations"] == sum(
        row["observations"] for row in load("02_quarterly_regions")
        if row["quarter"] == geography["period"]
    ) + sum(
        row["usable_dkp_rows"] for row in quality
        if row["quarter"] == geography["period"]
        and (row["region"], row["quarter"]) not in regional
    )
    for workers in (1, 2):
        path = args.results / f"14_benchmark_{workers}_worker.json"
        if path.exists():
            benchmark = json.loads(path.read_text(encoding="utf-8"))
            assert benchmark["observations"] == 1_952_424
            assert benchmark["fresh_aggregation_each_trial"]
    print("Input totals, geography, filters, source comparisons and available benchmarks are consistent.")


if __name__ == "__main__":
    main()
