"""Pure index math. No I/O, no venue knowledge."""
import numpy as np
import pandas as pd

from . import config


def clip_prob(p: np.ndarray, lo: float | None = None,
              hi: float | None = None) -> np.ndarray:
    return np.clip(np.asarray(p, dtype=float),
                   config.CLIP_LO if lo is None else lo,
                   config.CLIP_HI if hi is None else hi)


def logit(p: np.ndarray, lo: float | None = None,
          hi: float | None = None) -> np.ndarray:
    p = clip_prob(p, lo, hi)
    return np.log(p / (1.0 - p))


def binary_entropy(p: np.ndarray, lo: float | None = None,
                   hi: float | None = None) -> np.ndarray:
    p = clip_prob(p, lo, hi)
    return -(p * np.log2(p) + (1.0 - p) * np.log2(1.0 - p))


def ewma_vol(innov: pd.Series,
             halflife: float = config.EWMA_HALFLIFE_DAYS) -> pd.Series:
    """EWMA volatility of `innov`, decayed in CALENDAR DAYS.

    `innov` must be indexed by date and is expected to be gappy: a market
    leaves and re-enters the universe as its liquidity crosses the rolling
    floor or as the pin rule bites, and only the observed innovations are
    passed in. An observation-counted `ewm` would treat the pre-gap
    innovations as one observation old on the day the market returns, so a
    market that was violent before a three-month absence would re-enter
    carrying its old volatility at full weight — on exactly the news days
    that pull it back over the floor. `times=` decays by elapsed days
    instead, so an absence costs the state its proper number of halflives.
    """
    if innov.empty:
        return innov.astype(float)
    if not isinstance(innov.index, pd.DatetimeIndex):
        raise TypeError("ewma_vol needs a DatetimeIndex to decay in days")
    return (
        innov.pow(2)
        .ewm(halflife=pd.Timedelta(days=halflife), times=innov.index,
             min_periods=config.EWMA_MIN_PERIODS)
        .mean()
        .pow(0.5)
    )


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return float("nan")
    return float(np.average(values[mask], weights=weights[mask]))


def percentile_scale(raw: pd.Series, seed_days: int = config.SEED_DAYS) -> pd.Series:
    """Expanding percentile of each value vs strictly-prior history, 0-100.

    First seed_days *valid* values are NaN (seed period, not publishable).
    NaN entries in `raw` (no data that day) are excluded from the ranking
    entirely and stay NaN in the output - `raw=True` positional comparison
    would otherwise treat "NaN <= x" as a non-match, diluting percentiles
    around any gap and reporting 0.0 (not NaN) on a day with no value.
    """
    valid = raw.dropna()
    scaled = valid.expanding(min_periods=2).apply(
        lambda w: float((w[:-1] <= w[-1]).mean() * 100.0), raw=True
    )
    scaled.iloc[:seed_days] = np.nan
    return scaled.reindex(raw.index)
