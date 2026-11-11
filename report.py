import argparse
import html
import json
from pathlib import Path
from time import perf_counter

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


LABEL = {47: "Ленинградская область", 78: "Санкт-Петербург"}
COLOR = {47: "#9b4b2c", 78: "#146b72"}
NORTHWEST_NAMES = {
    10: "Республика Карелия", 11: "Республика Коми", 29: "Архангельская область",
    35: "Вологодская область", 39: "Калининградская область", 47: "Ленинградская область",
    51: "Мурманская область", 53: "Новгородская область", 60: "Псковская область",
    78: "Санкт-Петербург", 83: "Ненецкий АО",
}


def load(folder, name):
    return json.loads((folder / f"{name}.json").read_text(encoding="utf-8"))


def amount(value, decimals=0):
    return f"{value:,.{decimals}f}".replace(",", "\u00a0").replace(".", ",")


def save(figure, target):
    figure.savefig(target, dpi=155, bbox_inches="tight", facecolor="white")
    plt.close(figure)


def quarterly_chart(rows, target):
    fig, ax = plt.subplots(figsize=(9.3, 3.35), layout="constrained")
    for region in (78, 47):
        subset = [r for r in rows if r["region"] == region]
        values = [r["median_rub_m2"] / 1000 for r in subset]
        periods = [f"{(int(r['quarter'][5:7]) + 2) // 3} кв. {r['quarter'][2:4]}" for r in subset]
        ax.plot(periods, values, marker="o", linewidth=2.4, label=LABEL[region], color=COLOR[region])
    ax.set_ylabel("Медиана, тыс. ₽/м²")
    ax.set_xlabel("Квартал регистрации договора")
    ax.set_ylim(78, 211)
    ax.grid(axis="y", color="#e2e8ec")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, loc="upper left", ncols=2)
    save(fig, target)


def association_chart(rows, target):
    fig, ax = plt.subplots(figsize=(8.8, 2.8), layout="constrained")
    positions = {"median_building_year": (3.0, 3.55), "average_reported_deterioration": (1.0, 1.55)}
    for r in rows:
        index = 1 if r["region"] == 78 else 0
        y = positions[r["characteristic"]][index]
        x = r["within_district_rank_correlation"]
        lo, hi = r["district_bootstrap_95"]
        ax.errorbar(x, y, xerr=[[x - lo], [hi - x]], fmt="o", capsize=4, color=COLOR[r["region"]],
                    label=LABEL[r["region"]] if r["characteristic"] == "median_building_year" else None)
    ax.axvline(0, color="#8594a0", linewidth=1)
    ax.set_yticks([1.275, 3.275], ["Сообщённый износ", "Год постройки"])
    ax.set_ylim(0.55, 4.3)
    ax.set_xlim(-0.62, 0.67)
    ax.set_xlabel("Ранговая корреляция внутри кадастровых районов и 95% интервал")
    ax.grid(axis="x", color="#e2e8ec")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, loc="upper left", ncols=2)
    save(fig, target)


def geography_summary(results):
    national = load(results, "16_national_quarterly")
    latest = max(national, key=lambda row: row["quarter"])
    regions = sorted(
        [row for row in load(results, "02_quarterly_regions") if row["quarter"] == latest["quarter"]],
        key=lambda row: (-row["median_rub_m2"], row["region"]),
    )
    northwest = [
        {**row, "region_name": NORTHWEST_NAMES[row["region"]], "rank_in_observed_regions": rank,
         "relative_to_national_pct": round(100 * (row["median_rub_m2"] / latest["median_rub_m2"] - 1), 2)}
        for rank, row in enumerate(regions, start=1) if row["region"] in NORTHWEST_NAMES
    ]
    return {"period": latest["quarter"], "national": latest, "observed_regions": len(regions),
            "northwest_regions": northwest}


def geography_chart(summary, target):
    rows = list(reversed(summary["northwest_regions"]))
    fig, ax = plt.subplots(figsize=(9.2, 4.3), layout="constrained")
    ax.barh([row["region_name"] for row in rows], [row["median_rub_m2"] / 1000 for row in rows],
            color=[COLOR.get(row["region"], "#739da0") for row in rows])
    ax.axvline(summary["national"]["median_rub_m2"] / 1000, color="#193b4a", linestyle="--",
               label="Медиана по наблюдаемым ДКП России")
    ax.set_xlabel("Медианная цена ДКП, тыс. ₽/м²")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, fontsize=9, loc="lower right")
    save(fig, target)


def sources_chart(rows, target):
    recent = sorted(
        [r for r in rows if r["year"] == 2025 and r["relative_difference_pct"] is not None],
        key=lambda row: row["relative_difference_pct"],
    )
    fig, ax = plt.subplots(figsize=(8.8, 4.4), layout="constrained")
    ax.barh([r["city_ru"] for r in recent], [r["relative_difference_pct"] for r in recent],
            color=[COLOR[78] if r["city_name"] == "Saint Petersburg" else "#739da0" for r in recent])
    ax.set_xlabel("Различие медиан Циан и ДКП относительно ДКП, %")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x", color="#e2e8ec")
    ax.set_axisbelow(True)
    save(fig, target)


def write_view(target, quarterly, panel, cian, geography, sources, sensitivity, flags):
    def table(head, rows):
        wrap = lambda values, tag: "<tr>" + "".join(f"<{tag}>{html.escape(str(v))}</{tag}>" for v in values) + "</tr>"
        return "<table><thead>" + wrap(head, "th") + "</thead><tbody>" + "".join(wrap(r, "td") for r in rows) + "</tbody></table>"

    last = [r for r in quarterly if r["region"] in LABEL and r["quarter"] == "2026-01-01"]
    comparison = [r for r in panel if r["recent"] == "2026-01-01" and r["minimum_in_both_periods"] == 10]
    prices = table(("Регион", "₽/м²", "Наблюдения"),
                   ((LABEL[r["region"]], amount(r["median_rub_m2"]), amount(r["observations"])) for r in last))
    trend = table(("Регион", "По всем ДКП", "По сопоставимым группам", "Доля I кв. 2026"),
                  ((LABEL[r["region"]], amount(r["raw_change_pct"], 1) + "%",
                    amount(r["panel_change_pct"], 1) + "%", amount(r["coverage_recent_pct"], 1) + "%")
                   for r in comparison))
    offers = table(("Год", "ДКП, ₽/м²", "Геокластеры Циан, ₽/м²"),
                   ((r["year"], amount(r["median_rub_m2"]), amount(r["median_of_cluster_medians"])) for r in cian))
    regions = table(("Регион", "ДКП, ₽/м²", "Наблюдения", "Относительно общей медианы"),
                    ((r["region_name"], amount(r["median_rub_m2"]), amount(r["observations"]),
                      amount(r["relative_to_national_pct"], 1) + "%") for r in geography["northwest_regions"]))
    source_rows = table(("Город", "Год", "ДКП, ₽/м²", "Циан, ₽/м²", "Различие"),
                        ((r["city_ru"], r["year"],
                          amount(r["median_rub_m2"]) if r["observations"] else "нет данных",
                          amount(r["median_of_cluster_medians"]),
                          amount(r["relative_difference_pct"], 1) + "%"
                          if r["relative_difference_pct"] is not None else "нет данных")
                         for r in sources))
    scenarios = table(("Регион", "Сценарий", "Наблюдения", "Медиана, ₽/м²"),
                      ((LABEL[r["region"]], r["scenario"], amount(r["observations"]),
                        amount(r["median_rub_m2"])) for r in sensitivity if r["quarter"] == geography["period"]))
    flagged = table(("Регион", "Наблюдения", "За границами 1,5 IQR"),
                    ((LABEL[r["region"]], amount(r["observations"]), amount(r["tukey_flagged"]))
                     for r in flags if r["quarter"] == geography["period"]))
    target.write_text(f"""<!doctype html>
<html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Анализ цен сделок с жильём в России с фокусом на Санкт-Петербург и Ленинградскую область</title>
<style>
body{{font:17px/1.6 system-ui,sans-serif;color:#17303e;margin:40px auto;padding:0 22px;max-width:880px}}
h1,h2{{line-height:1.16}} h1{{font-size:2.1rem}} h2{{margin-top:2.3em}}
table{{border-collapse:collapse;width:100%;margin:1.3em 0;text-align:left}}
td,th{{padding:.48em .55em;border-bottom:1px solid #cedcde}}
th{{background:#eef4f4}} img{{max-width:100%;height:auto}} a{{color:#146b72}}
</style>
<h1>Анализ цен сделок с жильём в России с фокусом на Санкт-Петербург и Ленинградскую область</h1>
<p>Жилинко Злата, Бусламов Анис, Соловьев Кирилл · группа 60202</p>
<p>ДКП жилых помещений, I квартал 2024 — I квартал 2026. Входные данные по России — 2,19 ГБ.</p>
<h2>1. Пространственные различия цен</h2>
<p>Общая медиана по наблюдаемым ДКП России — {amount(geography["national"]["median_rub_m2"])} ₽/м²
по {amount(geography["national"]["observations"])} записям в I квартале 2026 года.</p>
<img src="geography.png" alt="Сравнение регионов Северо-Запада с общей медианой">
{regions}
<h2>2. Динамика на сопоставимых выборках</h2>
{prices}
<img src="quarterly.png" alt="Квартальные медианы Петербурга и Ленобласти">
{trend}
<p>В Ленобласти сопоставимая выборка с порогом 10 наблюдений охватила один кадастровый район.
Общая медиана меняется вместе с географией наблюдений.</p>
<h2>3. Качество и чувствительность к очистке</h2>
{scenarios}
<p>wide — 10–400 м² и 5–2 000 тыс. ₽/м²; medium — 20–200 м² и 20–1 000 тыс. ₽/м²;
narrow — 25–150 м² и 30–600 тыс. ₽/м². Эти сценарии меняют состав выборки.</p>
{flagged}
<p>Выход за границы 1,5 межквартильного размаха служит статистическим признаком необычной цены.
Для установления причины требуется проверка записи.</p>
<h2>4. Связь цен с жилфондом</h2>
<img src="housing.png" alt="Ранговые корреляции цен с годом постройки и износом">
<p>Один объект анализа — кадастровый квартал. Связи описывают территории.
На цену также могут влиять состояние квартиры, инфраструктура и другие факторы.</p>
<h2>5. Сделки и предложения в городах России</h2>
<img src="sources.png" alt="Различие ценовых сигналов Циан и ДКП по городам за 2025 год">
{source_rows}
<h3>Петербург в 2024–2025 годах</h3>
{offers}
<p>Циан публикует агрегаты предложений по геокластерам. Выгрузка ДКП описывает зарегистрированные договоры.</p>
</html>""", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    started = perf_counter()
    args.output.mkdir(parents=True, exist_ok=True)
    quarterly = load(args.results, "02_quarterly_regions")
    quarterly_chart(quarterly, args.output / "quarterly.png")
    association_chart(load(args.results, "12_housing_associations"), args.output / "housing.png")
    geography = geography_summary(args.results)
    (args.results / "20_geography_summary.json").write_text(
        json.dumps(geography, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    geography_chart(geography, args.output / "geography.png")
    sources = load(args.results, "18_city_source_comparison")
    sources_chart(sources, args.output / "sources.png")
    write_view(args.output / "index.html", quarterly, load(args.results, "11_matched_panel"),
               load(args.results, "09_spb_offer_vs_contract"), geography, sources,
               load(args.results, "17_filter_sensitivity"), load(args.results, "19_statistical_price_flags"))
    (args.results / "15_visualization_seconds.json").write_text(
        json.dumps({"visualization": round(perf_counter() - started, 3)}, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
