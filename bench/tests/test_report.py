from fraud_bench.report import knee_svg, render_table, summarize


def rep(rate, p95, p99=None, **over):
    base = {"rate": rate, "p50": p95 / 3, "p95": p95, "p99": p99 or p95 * 1.5, "gen_valid": True,
            "delivery_errors": 0, "lag_stable": True, "dlq_growth": 0, "lost": 0}
    return {**base, **over}


STEPS = [rep(1000, 20), rep(1000, 22), rep(1000, 24), rep(2000, 70), rep(2000, 75), rep(2000, 120),
         rep(5000, 900, lag_stable=False), rep(5000, 950, lag_stable=False)]


def test_summarize_groups_repetitions_by_rate_with_median_and_range():
    rows = summarize(STEPS)
    assert [r["rate"] for r in rows] == [1000, 2000, 5000]
    assert (rows[0]["p95_median"], rows[0]["p95_min"], rows[0]["p95_max"], rows[0]["reps"]) == (22, 20, 24, 3)
    assert rows[1]["p95_median"] == 75 and rows[1]["sustained"] is True    # median within budget
    assert rows[2]["sustained"] is False


def test_table_marks_sustained_rates_and_names_the_failing_condition():
    table = render_table(summarize(STEPS))
    assert "| 1,000 |" in table and "| 5,000 |" in table
    assert "22" in table and "20-24" in table
    last = [line for line in table.splitlines() if "5,000" in line][0]
    assert "lag_stable" in last and "p95_within_budget" in last


def test_knee_chart_is_an_svg_with_the_budget_line_and_one_point_per_rate():
    svg = knee_svg(summarize(STEPS))
    assert svg.startswith("<svg") and svg.rstrip().endswith("</svg>")
    assert svg.count("<circle") == 3 and "80 ms" in svg
