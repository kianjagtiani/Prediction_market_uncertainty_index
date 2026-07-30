"""Assemble the Phase 1 validation report."""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .. import config
from . import benchmarks, events

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
    constituents = pd.read_parquet(config.DATA_DIR / "indices" /
                                   "constituents.parquet")
    _index_chart(indices)

    ev = events.check_events(indices)
    spikes = events.top_spike_days(indices)
    bench = json.loads((config.DATA_DIR / "benchmarks" /
                        "comparison.json").read_text())
    robust_path = config.DATA_DIR / "indices" / "robustness.csv"
    robust = pd.read_csv(robust_path) if robust_path.exists() else None

    # churn audit: index moves must not track membership moves
    glob = indices[(indices["index"] == "GLOBAL") &
                   (indices["gauge"] == "turbulence")].set_index("date")["value"]
    n = constituents[constituents["index"] == "GLOBAL"
                     ].set_index("date")["n_constituents"]
    churn_corr = float(glob.diff().corr(n.diff()))

    lines = [
        "# Uncertainty Index — Phase 1 Validation Report", "",
        f"_Generated {pd.Timestamp.now():%Y-%m-%d}_", "",
        "![All indices](all_indices.png)", "",
        "## Event study (pass = window max >= 90)", "",
        ev.to_markdown(index=False), "",
        "## Top 10 spike days (GLOBAL turbulence)",
        "", "Annotate each date with the driving news story before publishing:",
        "", spikes.to_markdown(index=False), "",
        "## Benchmark comparison", "",
    ]
    for name, r in bench.items():
        lines.append(f"- **{name}**: level corr {r['level_corr']:.2f}, "
                     f"diff corr {r['diff_corr']:.2f}, "
                     f"best lag {benchmarks.best_lag(r['leadlag']):+d} "
                     f"(negative = we lead). ![chart](benchmark_{name.lower()}.png)")
    lines += ["", "## Robustness", ""]
    if robust is not None:
        lines.append(f"Min pairwise correlation across param variants: "
                     f"**{robust['corr'].min():.3f}** (target >= 0.90).")
    lines += ["", "## Churn audit", "",
              f"Corr(Δindex, Δconstituent-count) = **{churn_corr:.3f}** "
              f"(want |value| small; large means membership churn, not news, "
              f"moves the index)."]

    (OUT / "report.md").write_text("\n".join(lines))
    print(f"report written to {OUT / 'report.md'}")


if __name__ == "__main__":
    main()
