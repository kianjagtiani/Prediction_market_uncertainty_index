"""Pure index math. No I/O, no venue knowledge."""
import numpy as np
import pandas as pd

from . import config


def clip_prob(p: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(p, dtype=float), config.CLIP_LO, config.CLIP_HI)


def logit(p: np.ndarray) -> np.ndarray:
    p = clip_prob(p)
    return np.log(p / (1.0 - p))


def binary_entropy(p: np.ndarray) -> np.ndarray:
    p = clip_prob(p)
    return -(p * np.log2(p) + (1.0 - p) * np.log2(1.0 - p))


def ewma_vol(innov: pd.Series, halflife: float = config.EWMA_HALFLIFE_DAYS) -> pd.Series:
    return (
        innov.pow(2)
        .ewm(halflife=halflife, min_periods=config.EWMA_MIN_PERIODS)
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

    First seed_days values are NaN (seed period, not publishable).
    """
    scaled = raw.expanding(min_periods=2).apply(
        lambda w: float((w[:-1] <= w[-1]).mean() * 100.0), raw=True
    )
    scaled.iloc[:seed_days] = np.nan
    return scaled
