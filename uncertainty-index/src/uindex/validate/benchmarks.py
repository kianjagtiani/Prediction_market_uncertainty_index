"""Compare GLOBAL turbulence to VIX, EPU, GPR: correlation + lead-lag."""
import json
from pathlib import Path

import httpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .. import config

BENCH_DIR = config.DATA_DIR / "benchmarks"
CHART_DIR = config.PROJECT_ROOT / "docs" / "validation"

SOURCES = {
    "VIX": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS",
    "EPU": "https://www.policyuncertainty.com/media/All_Daily_Policy_Data.csv",
    "GPR": "https://www.matteoiacoviello.com/gpr_files/data_gpr_daily_recent.xls",
}


def align(index_series: pd.Series, bench: pd.Series) -> pd.DataFrame:
    joined = pd.concat({"idx": index_series, "bench": bench}, axis=1).dropna()
    return joined


def corr_and_leadlag(joined: pd.DataFrame, max_lag: int = 10) -> dict:
    diffs = joined.diff().dropna()
    leadlag = {
        lag: float(diffs["idx"].corr(diffs["bench"].shift(lag)))
        for lag in range(-max_lag, max_lag + 1)
    }
    return {
        "level_corr": float(joined["idx"].corr(joined["bench"])),
        "diff_corr": float(diffs["idx"].corr(diffs["bench"])),
        "leadlag": leadlag,
    }


def _download() -> dict[str, pd.Series]:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=60, follow_redirects=True)
    out = {}

    raw = BENCH_DIR / "vix.csv"
    raw.write_bytes(client.get(SOURCES["VIX"]).content)
    vix = pd.read_csv(raw, na_values=".")
    vix.columns = ["date", "vix"]
    out["VIX"] = vix.assign(date=pd.to_datetime(vix["date"])).set_index("date")["vix"]

    raw = BENCH_DIR / "epu.csv"
    raw.write_bytes(client.get(SOURCES["EPU"]).content)
    epu = pd.read_csv(raw)
    epu["date"] = pd.to_datetime(epu[["year", "month", "day"]])
    out["EPU"] = epu.set_index("date")["daily_policy_index"]

    raw = BENCH_DIR / "gpr.xls"
    raw.write_bytes(client.get(SOURCES["GPR"]).content)
    gpr = pd.read_excel(raw)
    gpr.columns = [c.lower() for c in gpr.columns]
    out["GPR"] = gpr.assign(date=pd.to_datetime(gpr["date"])).set_index("date")["gprd"]
    return out


def main() -> None:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    indices = pd.read_parquet(config.DATA_DIR / "indices" / "indices.parquet")
    ours = (indices[(indices["index"] == "GLOBAL") &
                    (indices["gauge"] == "turbulence")]
            .set_index("date")["value"].dropna())

    results = {}
    for name, bench in _download().items():
        joined = align(ours, bench)
        results[name] = corr_and_leadlag(joined)
        fig, ax1 = plt.subplots(figsize=(11, 4.5))
        ax1.plot(joined.index, joined["idx"], label="Global Uncertainty (0-100)",
                 linewidth=2)
        ax2 = ax1.twinx()
        ax2.plot(joined.index, joined["bench"], alpha=0.6, color="tab:orange",
                 label=name, linewidth=2)
        ax1.set_title(f"Global Uncertainty Index vs {name} "
                      f"(level corr {results[name]['level_corr']:.2f})")
        fig.legend(loc="upper left")
        fig.tight_layout()
        fig.savefig(CHART_DIR / f"benchmark_{name.lower()}.png", dpi=150)
        plt.close(fig)

    (BENCH_DIR / "comparison.json").write_text(json.dumps(results, indent=2))
    for name, r in results.items():
        best = max(r["leadlag"], key=r["leadlag"].get)
        print(f"{name}: level={r['level_corr']:.2f} diff={r['diff_corr']:.2f} "
              f"best lag={best:+d} (negative = we lead)")


if __name__ == "__main__":
    main()
