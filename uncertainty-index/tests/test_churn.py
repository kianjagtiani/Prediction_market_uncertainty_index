import pandas as pd
import pytest

from uindex.validate import churn

DATES = pd.date_range("2024-01-01", periods=30, freq="D")

# Synthetic flagged panel (universe internals bypassed on purpose):
# A: constant price -> zero vol, weight 1, eligible for the whole sample.
# B: whale (weight 100), oscillating price, eligible from day 8; with
#    EWMA_MIN_PERIODS=5 its vol first exists on day 13 (pure membership
#    event). From day 20 its oscillation widens (pure repricing event).


def _flagged():
    rows = [{"date": d, "market_id": "A", "close_prob": 0.5, "weight": 1.0,
             "eligible_turbulence": True, "category": "WAR"} for d in DATES]
    for i, d in enumerate(DATES):
        if i < 8:
            continue
        if i < 20:
            p = 0.4 if i % 2 == 0 else 0.6
        else:
            p = 0.9 if i % 2 == 0 else 0.1
        rows.append({"date": d, "market_id": "B", "close_prob": p,
                     "weight": 100.0, "category": "WAR",
                     "eligible_turbulence": True})
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def daily():
    return churn.decompose(_flagged())


def test_identity_delta_equals_sum_of_components(daily):
    assert (daily["delta_raw"]
            - daily[churn.COMPONENTS].sum(axis=1)).abs().max() < 1e-12


def _stable_vols():
    """Two markets alternating between fixed prices: |Δlogit| is constant,
    so each EWMA vol is exactly flat and any index move must be weights."""
    rows = [{"date": d, "market_id": mid, "weight": 1.0,
             "close_prob": lo if i % 2 == 0 else hi,
             "eligible_turbulence": True, "category": "WAR"}
            for mid, lo, hi in (("A", 0.45, 0.55), ("B", 0.30, 0.70))
            for i, d in enumerate(DATES)]
    return pd.DataFrame(rows)


def test_pure_weight_drift_is_reweighting_not_repricing():
    """Identical prices and identical membership on both days; only one
    market's liquidity weight jumps. The two-way split booked all of that
    as 'news moving prices of the standing membership'."""
    flagged = _stable_vols()
    jump = (flagged["market_id"] == "B") & (flagged["date"] >= DATES[25])
    flagged.loc[jump, "weight"] = 40.0
    row = churn.decompose(flagged).loc[DATES[25]]
    assert abs(row["delta_raw"]) > 1e-6
    assert row["reweighting"] == pytest.approx(row["delta_raw"])
    assert row["repricing"] == pytest.approx(0.0, abs=1e-12)
    assert row["membership"] == pytest.approx(0.0, abs=1e-12)


def test_membership_dominates_on_entry_day(daily):
    row = daily.loc[DATES[13]]  # B's vol first defined: entering whale
    assert row["delta_raw"] > 0
    assert row["repricing"] == pytest.approx(0.0, abs=1e-12)  # A's vol is flat
    assert row["membership"] == pytest.approx(row["delta_raw"])


def test_repricing_dominates_on_stable_membership_day(daily):
    row = daily.loc[DATES[20]]  # B swings harder, nobody enters or exits
    assert abs(row["repricing"]) > 1e-6
    assert row["membership"] == pytest.approx(0.0, abs=1e-12)
    assert row["repricing"] == pytest.approx(row["delta_raw"])


def test_shares_are_bounded(daily):
    s = churn.shares(daily)
    assert set(s) == set(churn.COMPONENTS)
    assert all(0.0 <= v <= 1.0 for v in s.values())
