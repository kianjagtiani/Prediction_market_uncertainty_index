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
             "eligible": True, "category": "WAR"} for d in DATES]
    for i, d in enumerate(DATES):
        if i < 8:
            continue
        if i < 20:
            p = 0.4 if i % 2 == 0 else 0.6
        else:
            p = 0.9 if i % 2 == 0 else 0.1
        rows.append({"date": d, "market_id": "B", "close_prob": p,
                     "weight": 100.0, "eligible": True, "category": "WAR"})
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def daily():
    return churn.decompose(_flagged())


def test_identity_delta_equals_repricing_plus_membership(daily):
    assert (daily["delta_raw"]
            - daily["repricing"] - daily["membership"]).abs().max() < 1e-12


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


def test_membership_share_bounded(daily):
    share = churn.membership_share(daily)
    assert 0.0 <= share <= 1.0
