"""Assemble the Phase 1 validation report."""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .. import config
from . import benchmarks, churn, events

OUT = config.PROJECT_ROOT / "docs" / "validation"


def _index_chart(indices: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    for name in config.INDEXES:
        sub = indices[(indices["index"] == name) &
                      (indices["gauge"] == "turbulence")].dropna(subset=["value"])
        lw = 2.5 if name == "GLOBAL" else 0.9
        ax.plot(sub["date"], sub["value"], label=name, linewidth=lw)
    ax.set_title("Turbulence indices, 0-100 percentile scale")
    ax.legend(ncol=4, fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT / "all_indices.png", dpi=150)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    indices = pd.read_parquet(config.DATA_DIR / "indices" / "indices.parquet")
    _index_chart(indices)

    ev = events.check_events(indices)
    spikes = events.top_spike_days(indices)
    bench = json.loads((config.DATA_DIR / "benchmarks" /
                        "comparison.json").read_text())
    robust_path = config.DATA_DIR / "indices" / "robustness.csv"
    robust = pd.read_csv(robust_path) if robust_path.exists() else None
    churn_path = config.DATA_DIR / "indices" / "churn.csv"
    churn_daily = pd.read_csv(churn_path) if churn_path.exists() else None

    lines = [
        "# Uncertainty Index — Phase 1 Validation Report", "",
        f"_Generated {pd.Timestamp.now():%Y-%m-%d}_", "",
        "![All indices](all_indices.png)", "",
        "## Event study (pass = placebo p-value <= 0.10)", "",
        "p-value = share of equal-length non-event windows whose max >= the "
        "event window's max; `max_ge_90` is the legacy threshold check.", "",
        ev.to_markdown(index=False), "",
        "## Top 10 spike days (GLOBAL turbulence)",
        "", "Annotate each date with the driving news story before publishing:",
        "", spikes.to_markdown(index=False), "",
        "## Benchmark comparison", "",
    ]
    for name, r in bench.items():
        lines.append(f"- **{name}**: level corr {r['level_corr']:.2f}, "
                     f"diff corr {r['diff_corr']:.2f}, "
                     f"best lag {benchmarks.best_lag(r['leadlag']):+d}, "
                     f"leads: {'yes' if r['leads'] else 'no'} "
                     f"(noise band {r['noise_band']:.3f}; a lead is claimed "
                     f"only if best-lag corr beats lag-0 by the band). "
                     f"![chart](benchmark_{name.lower()}.png)")
    lines += ["", "## Robustness", ""]
    if robust is not None:
        lines += [f"Min pairwise correlation across param variants: "
                  f"**{robust['min_pairwise_corr'].min():.3f}** "
                  f"(target >= 0.90). Per-variant event-study passes:", "",
                  robust.to_markdown(index=False)]
    lines += ["", "## Churn audit", ""]
    if churn_daily is not None:
        share = churn.membership_share(churn_daily)
        lines.append(f"Membership share of total |Δraw| (GLOBAL turbulence) = "
                     f"**{share:.3f}** (guideline <= 0.20; large means "
                     f"membership churn, not repricing, moves the index).")
    else:
        lines.append("churn.csv not found — run `python -m uindex.validate.churn`.")

    (OUT / "report.md").write_text("\n".join(lines))
    print(f"report written to {OUT / 'report.md'}")


if __name__ == "__main__":
    main()
