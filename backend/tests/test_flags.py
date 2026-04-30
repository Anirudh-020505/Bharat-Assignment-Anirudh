"""
tests/test_flags.py — Unit tests for all 8 flag detection functions.

At least one positive (flag=True) and one negative (flag=False) case per flag.
Run: python -m pytest tests/test_flags.py -v
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from src.flags import (
    flag_suspected_unit_error,
    flag_rate_overlap,
    flag_timezone_boundary_risk,
    flag_invalid_hours,
    flag_backdated_crosses_cycle,
    flag_no_matching_transfer,
    flag_ambiguous_name_match,
    flag_rate_precision_anomaly,
    build_review_reason,
    apply_unit_error_flag,
    apply_rate_overlap_flag,
    apply_invalid_hours_flag,
    apply_backdated_flag,
)


# ---------------------------------------------------------------------------
# Bug 1: suspected_unit_error
# ---------------------------------------------------------------------------

class TestSuspectedUnitError:
    def test_positive_known_utr(self):
        """UTR00001459 amount 1800 paise = ₹18 — below ₹50 threshold."""
        assert flag_suspected_unit_error(Decimal("1800")) is True

    def test_positive_boundary_minus_one(self):
        """4999 paise = ₹49.99 — just under threshold."""
        assert flag_suspected_unit_error(Decimal("4999")) is True

    def test_negative_normal_transfer(self):
        """225000 paise = ₹2250 — normal wage payment."""
        assert flag_suspected_unit_error(Decimal("225000")) is False

    def test_negative_exact_threshold(self):
        """5000 paise = ₹50 — at threshold, should NOT flag (< not <=)."""
        assert flag_suspected_unit_error(Decimal("5000")) is False

    def test_apply_vectorized(self):
        df = pd.DataFrame({"amount_paise": [Decimal("1800"), Decimal("225000")]})
        result = apply_unit_error_flag(df)
        assert result["flag_suspected_unit_error"].tolist() == [True, False]


# ---------------------------------------------------------------------------
# Bug 2: rate_overlap
# ---------------------------------------------------------------------------

class TestRateOverlap:
    def test_positive_multiple_matches(self):
        """match_count=2 means overlap; most recent effective_from wins."""
        assert flag_rate_overlap(2) is True

    def test_positive_three_matches(self):
        assert flag_rate_overlap(3) is True

    def test_negative_single_match(self):
        """Exactly one rate match — no overlap."""
        assert flag_rate_overlap(1) is False

    def test_negative_no_match(self):
        """0 = unmatched worker, not an overlap."""
        assert flag_rate_overlap(0) is False

    def test_apply_vectorized(self):
        df = pd.DataFrame({"rate_match_count": [1, 2, 3, 0]})
        result = apply_rate_overlap_flag(df)
        assert result["flag_rate_overlap"].tolist() == [False, True, True, False]


# ---------------------------------------------------------------------------
# Bug 3: timezone_boundary_risk
# ---------------------------------------------------------------------------

class TestTimezoneBoundaryRisk:
    BOUNDARIES = [date(2025, 3, 15)]

    def _ts(self, dt_str: str) -> pd.Timestamp:
        return pd.Timestamp(dt_str).tz_localize("Asia/Kolkata")

    def test_positive_vendor_b_crosses_boundary(self):
        """vendor_b UTC entry: IST date=Mar 16, work_date=Mar 14 → crosses Mar 15 boundary."""
        entered_ist = self._ts("2025-03-16 04:00:00")
        assert flag_timezone_boundary_risk(
            entered_ist, date(2025, 3, 14), self.BOUNDARIES, "vendor_b_v1.0"
        ) is True

    def test_negative_vendor_a_skipped(self):
        """vendor_a_v2.3 is already IST — skip timezone check."""
        entered_ist = self._ts("2025-03-16 04:00:00")
        assert flag_timezone_boundary_risk(
            entered_ist, date(2025, 3, 14), self.BOUNDARIES, "vendor_a_v2.3"
        ) is False

    def test_negative_same_day(self):
        """Same day — no boundary crossed."""
        entered_ist = self._ts("2025-03-15 10:00:00")
        assert flag_timezone_boundary_risk(
            entered_ist, date(2025, 3, 15), self.BOUNDARIES, "vendor_b_v1.0"
        ) is False

    def test_negative_no_boundary_in_range(self):
        """Dates differ but no boundary between them."""
        entered_ist = self._ts("2025-03-12 04:00:00")
        assert flag_timezone_boundary_risk(
            entered_ist, date(2025, 3, 13), self.BOUNDARIES, "vendor_b_v1.0"
        ) is False

    def test_negative_none_timestamp(self):
        assert flag_timezone_boundary_risk(
            None, date(2025, 3, 15), self.BOUNDARIES, "vendor_b_v1.0"
        ) is False


# ---------------------------------------------------------------------------
# Bug 4: invalid_hours
# ---------------------------------------------------------------------------

class TestInvalidHours:
    def test_positive_known_L02617(self):
        """L02617 has hours=450 — clearly invalid."""
        assert flag_invalid_hours(Decimal("450")) is True

    def test_positive_zero(self):
        """0 hours — invalid (must be > 0)."""
        assert flag_invalid_hours(Decimal("0")) is True

    def test_positive_negative(self):
        """Negative hours — invalid."""
        assert flag_invalid_hours(Decimal("-1")) is True

    def test_negative_normal_shift(self):
        """8 hours — valid."""
        assert flag_invalid_hours(Decimal("8")) is False

    def test_negative_max_boundary(self):
        """Exactly 24 hours — valid (boundary inclusive)."""
        assert flag_invalid_hours(Decimal("24")) is False

    def test_negative_fractional(self):
        """7.5 hours — valid."""
        assert flag_invalid_hours(Decimal("7.5")) is False

    def test_apply_vectorized(self):
        df = pd.DataFrame({"hours": [Decimal("8"), Decimal("450"), Decimal("0")]})
        result = apply_invalid_hours_flag(df)
        assert result["flag_invalid_hours"].tolist() == [False, True, True]


# ---------------------------------------------------------------------------
# Bug 5: backdated_crosses_cycle
# ---------------------------------------------------------------------------

class TestBackdatedCrossesCycle:
    def _ts(self, dt_str: str) -> pd.Timestamp:
        return pd.Timestamp(dt_str).tz_localize("Asia/Kolkata")

    def test_positive_L02611_pattern(self):
        """Feb work_date, entered late March — crosses month boundary, lag=40+."""
        entered = self._ts("2025-03-25 19:30:00")
        assert flag_backdated_crosses_cycle(40, date(2025, 2, 10), entered) is True

    def test_negative_same_month(self):
        """5 days late but same month — not a backdating issue."""
        entered = self._ts("2025-02-15 19:30:00")
        assert flag_backdated_crosses_cycle(5, date(2025, 2, 10), entered) is False

    def test_negative_within_7_days(self):
        """7 days lag but crosses month — lag threshold not exceeded."""
        entered = self._ts("2025-02-01 19:30:00")
        assert flag_backdated_crosses_cycle(7, date(2025, 1, 25), entered) is False

    def test_negative_none_lag(self):
        """Missing lag — cannot flag."""
        assert flag_backdated_crosses_cycle(None, date(2025, 2, 1), None) is False


# ---------------------------------------------------------------------------
# Bug 6: no_matching_transfer
# ---------------------------------------------------------------------------

class TestNoMatchingTransfer:
    def test_positive_expected_no_paid(self):
        assert flag_no_matching_transfer(Decimal("150000"), Decimal("0")) is True

    def test_negative_paid_exists(self):
        assert flag_no_matching_transfer(Decimal("150000"), Decimal("150000")) is False

    def test_negative_both_zero(self):
        """Worker with no shifts and no transfer — not flagged."""
        assert flag_no_matching_transfer(Decimal("0"), Decimal("0")) is False

    def test_negative_only_paid_no_expected(self):
        """Transfer exists but no shifts — not this flag's job."""
        assert flag_no_matching_transfer(Decimal("0"), Decimal("100000")) is False


# ---------------------------------------------------------------------------
# Bug 7: ambiguous_name_match
# ---------------------------------------------------------------------------

class TestAmbiguousNameMatch:
    def test_positive_two_candidates(self):
        candidates = json.dumps([
            {"worker_id": "W0001", "name": "Ramesh Kumar", "score": 92},
            {"worker_id": "W0002", "name": "Ramesh Kumar", "score": 90},
        ])
        assert flag_ambiguous_name_match(candidates) is True

    def test_negative_single_candidate(self):
        candidates = json.dumps([
            {"worker_id": "W0001", "name": "Ramesh Kumar", "score": 92},
        ])
        assert flag_ambiguous_name_match(candidates) is False

    def test_negative_none(self):
        assert flag_ambiguous_name_match(None) is False

    def test_negative_empty_string(self):
        assert flag_ambiguous_name_match("") is False

    def test_negative_malformed_json(self):
        """Malformed JSON → no flag (safe failure)."""
        assert flag_ambiguous_name_match("{bad json") is False


# ---------------------------------------------------------------------------
# Bug 8: rate_precision_anomaly
# ---------------------------------------------------------------------------

class TestRatePrecisionAnomaly:
    def test_positive_known_crop_inspector(self):
        """Crop Inspector MH junior = ₹450.33 — non-integer."""
        assert flag_rate_precision_anomaly(Decimal("450.33")) is True

    def test_negative_integer_rate(self):
        """₹300.00 — integer rate."""
        assert flag_rate_precision_anomaly(Decimal("300.00")) is False

    def test_negative_none(self):
        """No rate matched — no flag."""
        assert flag_rate_precision_anomaly(None) is False

    def test_negative_round_decimal(self):
        """₹520.00 — whole number stored as Decimal."""
        assert flag_rate_precision_anomaly(Decimal("520.00")) is False


# ---------------------------------------------------------------------------
# review_reason composer
# ---------------------------------------------------------------------------

class TestBuildReviewReason:
    def test_single_flag(self):
        row = pd.Series({"flag_rate_overlap": True, "flag_invalid_hours": False})
        from src.flags import SHIFT_FLAGS
        result = build_review_reason(row, SHIFT_FLAGS)
        assert "rate_overlap" in result
        assert "invalid_hours" not in result

    def test_multiple_flags(self):
        row = pd.Series({
            "flag_rate_overlap": True,
            "flag_invalid_hours": True,
            "flag_timezone_boundary_risk": False,
            "flag_backdated_crosses_cycle": False,
            "flag_ambiguous_name_match": False,
            "flag_rate_precision_anomaly": False,
        })
        from src.flags import SHIFT_FLAGS
        result = build_review_reason(row, SHIFT_FLAGS)
        assert result == "rate_overlap,invalid_hours"

    def test_no_flags(self):
        row = pd.Series({col: False for _, col in __import__("src.flags", fromlist=["SHIFT_FLAGS"]).SHIFT_FLAGS})
        from src.flags import SHIFT_FLAGS
        result = build_review_reason(row, SHIFT_FLAGS)
        assert result == ""
