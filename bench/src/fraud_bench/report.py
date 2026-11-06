"""Turn a run's raw steps into the README results table and the 'knee' chart (PLAN.md sections 5.4, 5.9)."""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path

from fraud_bench.analysis import P95_BUDGET_MS, median_range, step_conditions, sustained_rate
from fraud_bench.stack import ROOT


def summarize(steps: list[dict]) -> list[dict]:
    by_rate: dict[int, list[dict]] = defaultdict(list)
    for s in steps:
        by_rate[s["rate"]].append(s)
    best = sustained_rate(by_rate)
    rows = []
    for rate in sorted(by_rate):
        reps = by_rate[rate]
        p95 = median_range([r["p95"] for r in reps])
        failing = sorted({name for r in reps for name, ok in step_conditions(r).items() if not ok})
        # a rate passes when the median p95 is within budget and no repetition fails another condition
        other_fail = sorted({name for r in reps for name, ok in step_conditions(r).items()
                             if not ok and name != "p95_within_budget"})
        rows.append({
            "rate": rate, "reps": len(reps),
            "p50_median": median_range([r["p50"] for r in reps])[0],
            "p95_median": p95[0], "p95_min": p95[1], "p95_max": p95[2],
            "p99_median": median_range([r["p99"] for r in reps])[0],
            "failing": failing if (p95[0] > P95_BUDGET_MS or other_fail) else [],
            "sustained": p95[0] <= P95_BUDGET_MS and not other_fail,
            "is_best": rate == best,
        })
    return rows


def _num(x: float) -> str:
    return f"{x:,.0f}" if x >= 100 else f"{x:.1f}".rstrip("0").rstrip(".")


def render_table(rows: list[dict]) -> str:
    out = ["| rate (ev/s) | reps | p50 (ms) | p95 (ms) | p95 range | p99 (ms) | sustained | failing |",
           "|---|---|---|---|---|---|---|---|"]
    for r in rows:
        mark = "yes (highest)" if r["is_best"] else ("yes" if r["sustained"] else "no")
        out.append(f"| {r['rate']:,} | {r['reps']} | {_num(r['p50_median'])} | {_num(r['p95_median'])} | "
                   f"{_num(r['p95_min'])}-{_num(r['p95_max'])} | {_num(r['p99_median'])} | {mark} | "
                   f"{', '.join(r['failing']) or '-'} |")
    return "\n".join(out)


def knee_svg(rows: list[dict], width: int = 640, height: int = 360) -> str:
    """p95 vs offered rate on log-y with the latency budget as a dashed line (no plotting dependency)."""
    left, right, top, bottom = 60, 20, 20, 50
    xs = [r["rate"] for r in rows]
    ys = [max(r["p95_median"], 1.0) for r in rows]
    ymin, ymax = 1.0, 10 ** math.ceil(math.log10(max(ys + [P95_BUDGET_MS * 2])))

    def px(rate: float) -> float:
        span = (max(xs) - min(xs)) or 1
        return left + (rate - min(xs)) / span * (width - left - right)

    def py(ms: float) -> float:
        frac = (math.log10(ms) - math.log10(ymin)) / (math.log10(ymax) - math.log10(ymin))
        return height - bottom - frac * (height - top - bottom)

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" '
             f'aria-label="p95 latency versus offered rate">',
             '<style>text{font:12px sans-serif;fill:#333}.ax{stroke:#999}.ln{fill:none;stroke:#2563eb;'
             'stroke-width:2}.bud{stroke:#dc2626;stroke-dasharray:6 4}</style>',
             f'<line class="ax" x1="{left}" y1="{height - bottom}" x2="{width - right}" '
             f'y2="{height - bottom}"/>',
             f'<line class="ax" x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}"/>']
    decade = 1.0
    while decade <= ymax:
        parts.append(f'<text x="{left - 8}" y="{py(decade) + 4}" text-anchor="end">{decade:,.0f}</text>')
        decade *= 10
    pts = " ".join(f"{px(x):.1f},{py(y):.1f}" for x, y in zip(xs, ys, strict=True))
    parts.append(f'<line class="bud" x1="{left}" y1="{py(P95_BUDGET_MS):.1f}" x2="{width - right}" '
                 f'y2="{py(P95_BUDGET_MS):.1f}"/>')
    parts.append(f'<text x="{width - right}" y="{py(P95_BUDGET_MS) - 6:.1f}" text-anchor="end">'
                 f'{P95_BUDGET_MS:.0f} ms budget</text>')
    parts.append(f'<polyline class="ln" points="{pts}"/>')
    for x, y in zip(xs, ys, strict=True):
        parts.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="4" fill="#2563eb"/>')
        parts.append(f'<text x="{px(x):.1f}" y="{height - bottom + 18}" text-anchor="middle">{x:,}</text>')
    parts.append(f'<text x="{(left + width - right) / 2}" y="{height - 8}" text-anchor="middle">'
                 f'offered rate (events/s)</text>')
    parts.append(f'<text transform="translate(14 {(top + height - bottom) / 2}) rotate(-90)" '
                 f'text-anchor="middle">p95 end-to-end (ms, log)</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser(description="Render a bench run into a table and a knee chart")
    ap.add_argument("run_dir", type=Path, help="docs/results/<run_id>")
    args = ap.parse_args()
    run_dir = args.run_dir if args.run_dir.is_absolute() else ROOT / args.run_dir
    steps = json.loads((run_dir / "steps.json").read_text(encoding="utf-8"))
    rows = summarize(steps)
    table = render_table(rows)
    (run_dir / "report.md").write_text(table + "\n", encoding="utf-8")
    (run_dir / "knee.svg").write_text(knee_svg(rows), encoding="utf-8")
    print(table)
    best = next((r["rate"] for r in rows if r["is_best"]), None)
    print(f"\nsustained throughput (all four conditions, p95 <= {P95_BUDGET_MS:.0f} ms): "
          f"{'none' if best is None else f'{best:,} ev/s'}")


if __name__ == "__main__":
    main()
