"""docs/resilience/<id>.md and <id>.svg: the hypothesis, what happened, and whether it held."""
from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

from fraud_chaos.experiment import Experiment, Outcome
from fraud_chaos.verify import Timeline


def timeline_svg(tl: Timeline, budget_ms: float, width: int = 720, height: int = 300) -> str:
    """Decisions per second (bars) and p95 (line) around the fault, which sits at x = 0."""
    left, right, top, bottom = 52, 52, 24, 38
    xs = tl.seconds
    x0, x1 = xs[0], xs[-1]
    max_c = max(tl.count + [1])
    finite = [p for p in tl.p95_ms if not math.isnan(p)]
    max_p = max([budget_ms * 2, *finite]) if finite else budget_ms * 2

    def x(s: float) -> float:
        return left + (s - x0) / (x1 - x0) * (width - left - right)

    def yc(c: float) -> float:
        return height - bottom - c / max_c * (height - top - bottom)

    def yp(p: float) -> float:
        return height - bottom - min(p, max_p) / max_p * (height - top - bottom)

    bar_w = max(1.0, (width - left - right) / len(xs))
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" '
             f'aria-label="Decisions per second and p95 around the fault">',
             "<style>text{font:11px sans-serif;fill:#444}.b{fill:#9ecae1}.p{fill:none;stroke:#d94801;"
             "stroke-width:1.6}.f{stroke:#222;stroke-dasharray:4 3}"
             ".s{stroke:#2a9d8f;stroke-dasharray:2 3}</style>",
             f'<rect width="{width}" height="{height}" fill="#fff"/>']
    for s, c in zip(xs, tl.count, strict=True):
        if c:
            parts.append(f'<rect class="b" x="{x(s):.1f}" y="{yc(c):.1f}" width="{bar_w:.1f}" '
                         f'height="{height - bottom - yc(c):.1f}"/>')
    pts, run = [], []
    for s, p in zip(xs, tl.p95_ms, strict=True):
        if math.isnan(p):
            if run:
                pts.append(run)
            run = []
        else:
            run.append(f"{x(s):.1f},{yp(p):.1f}")
    if run:
        pts.append(run)
    parts += [f'<polyline class="p" points="{" ".join(r)}"/>' for r in pts]
    parts += [f'<line class="f" x1="{x(0):.1f}" x2="{x(0):.1f}" y1="{top}" y2="{height - bottom}"/>',
              f'<text x="{x(0) + 4:.1f}" y="{top + 10}">fault</text>',
              f'<line class="s" x1="{left}" x2="{width - right}" '
             f'y1="{yp(budget_ms):.1f}" y2="{yp(budget_ms):.1f}"/>',
              f'<text x="{left + 4}" y="{yp(budget_ms) - 3:.1f}">p95 budget {budget_ms:.0f} ms</text>',
              f'<text x="{left}" y="{height - 12}">seconds from the fault ({x0} to {x1})</text>',
              f'<text x="4" y="{top + 10}">{max_c}/s</text>',
              f'<text x="{width - right + 4}" y="{top + 10}">{max_p:.0f} ms</text>', "</svg>"]
    return "\n".join(parts)


def write_report(exp: Experiment, out: Outcome, confirmed: bool, checks: list[str], out_dir: Path,
                 fault_s: float) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    v = out.verification
    (out_dir / f"{exp.id}.svg").write_text(timeline_svg(out.timeline, exp.budget_ms), encoding="utf-8")
    (out_dir / f"{exp.id}.json").write_text(json.dumps({
        "id": exp.id, "confirmed": confirmed, "fault": exp.fault, "rate": exp.rate, "produced": v.produced,
        "lost": v.lost_count, "duplicated_extra": v.duplicate_extra, "recovery_s": out.recovery_s,
        "longest_gap_s": round(out.longest_gap_s, 2), "baseline_p95_ms": out.baseline_p95_ms,
        "degraded_share_after": round(out.degraded_share_after, 4), "dlq_growth": out.dlq_growth,
        "lost_sample": v.lost[:20], "run_at": datetime.fromtimestamp(fault_s).isoformat(timespec="seconds"),
    }, indent=2), encoding="utf-8")
    recovery = "never inside the window" if out.recovery_s is None else f"{out.recovery_s:.0f} s"
    lines = [
        f"# {exp.id} - {exp.title}", "",
        f"**Hypothesis (written before the run):** {exp.hypothesis}", "",
        f"**Result: hypothesis {'CONFIRMED' if confirmed else 'REFUTED'}.**", "",
        f"**Mechanism under test:** {exp.mechanism}" if exp.mechanism else "", "",
        "## Method", "",
        f"Steady load of {exp.rate} ev/s (about half of the sustained rate) for {exp.fault_at_s} s, "
        "then the fault, "
        f"then {exp.observe_s} s more. Fault: {out.extra.get('fault', '')}. After the load stopped the "
        "decisions topic was drained and every produced `payment_id` was compared with the decisions "
        "(`chaos/src/fraud_chaos/verify.py`).", "",
        "## Numbers", "",
        "| measure | value |", "|---|---|",
        f"| payments produced | {v.produced} |",
        f"| lost (produced, never decided) | {v.lost_count} |",
        f"| duplicated decisions (extra copies) | {v.duplicate_extra} |",
        f"| recovery (rate back and p95 back to normal for 5 s; normal = max({exp.budget_ms:.0f} ms, "
        f"1.5 x the pre-fault median p95)) | {recovery} |",
        f"| longest silence of the decisions topic | {out.longest_gap_s:.1f} s |",
        f"| p95 before the fault (last {exp.fault_at_s} s) | {out.baseline_p95_ms:.1f} ms |",
        f"| p95 after the fault, judged window | {out.p95_after_ms:.1f} ms |",
        f"| p95 in the rest of the observation | {out.p95_rest_ms:.1f} ms |",
        f"| degraded decisions after the fault | {out.degraded_share_after:.0%} |",
        f"| records added to the DLQ | {out.dlq_growth} |", "",
        "## Checks", "", *[f"- {c}" for c in checks], "",
        f"![decisions per second and p95 around the fault]({exp.id}.svg)", "",
        "The dashed vertical line is the fault. Bars are decisions per second, the orange line is the p95 of "
        "end-to-end latency, the green dotted line is the 80 ms budget.", ""]
    if v.lost:
        lines += ["## Lost payments (first 10)", "", "```", *v.lost[:10], "```", ""]
    path = out_dir / f"{exp.id}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
