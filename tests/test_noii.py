"""
Unit tests for the NOII pipeline: serialization, deserialization, inserter
price handling, and itch_runner --publish flag logic.

No external services (Kafka, Postgres) are required.
"""

import math
import datetime
import unittest

import pytest

from book.noii import Noii
from publishers.noii_publisher import _serialize_noii
from consumers.deserializers import deserialize_noii
from db.inserter import _nan_to_none


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_noii(**kwargs) -> Noii:
    defaults = dict(
        timestamp_ns=34_200_000_000_000,   # 9:30:00 ET in nanoseconds
        stock="AAPL",
        paired_shares=100_000,
        imbalance_shares=5_000,
        imbalance_direction="B",
        far_price=182.50,
        near_price=182.25,
        current_reference_price=182.00,
        cross_type="O",
        price_variation_indicator="L",
        trade_date=datetime.date(2024, 4, 1),
    )
    return Noii(**{**defaults, **kwargs})


# ---------------------------------------------------------------------------
# Serialization / deserialization roundtrip
# ---------------------------------------------------------------------------

class TestNoiiSerializationRoundtrip(unittest.TestCase):

    def test_all_fields_survive_roundtrip(self):
        noii = _make_noii()
        result = deserialize_noii(_serialize_noii(noii))

        assert result["ts_me"] == noii.timestamp_ns
        assert result["sec_id"] == noii.stock
        assert result["paired_shares"] == noii.paired_shares
        assert result["imbalance_shares"] == noii.imbalance_shares
        assert result["far_price"] == pytest.approx(noii.far_price)
        assert result["near_price"] == pytest.approx(noii.near_price)
        assert result["current_reference_price"] == pytest.approx(noii.current_reference_price)
        assert result["imbalance_direction"] == noii.imbalance_direction
        assert result["cross_type"] == noii.cross_type
        assert result["price_variation_indicator"] == noii.price_variation_indicator

    def test_closing_cross_fields(self):
        noii = _make_noii(cross_type="C", imbalance_direction="S",
                          price_variation_indicator="3")
        result = deserialize_noii(_serialize_noii(noii))
        assert result["cross_type"] == "C"
        assert result["imbalance_direction"] == "S"
        assert result["price_variation_indicator"] == "3"

    def test_no_imbalance_direction(self):
        noii = _make_noii(imbalance_direction="N", cross_type="O")
        result = deserialize_noii(_serialize_noii(noii))
        assert result["imbalance_direction"] == "N"

    def test_short_stock_symbol_is_padded_then_stripped(self):
        """Single-char symbols are padded to 8 bytes on the wire, stripped back on read."""
        noii = _make_noii(stock="X")
        result = deserialize_noii(_serialize_noii(noii))
        assert result["sec_id"] == "X"

    def test_eight_char_stock_symbol(self):
        noii = _make_noii(stock="ABCDEFGH")
        result = deserialize_noii(_serialize_noii(noii))
        assert result["sec_id"] == "ABCDEFGH"

    def test_msg_seq_is_positive(self):
        result = deserialize_noii(_serialize_noii(_make_noii()))
        assert result["msg_seq"] > 0

    def test_payload_size_is_fixed(self):
        """All NOII messages must have the same byte length for stream framing."""
        size_a = len(_serialize_noii(_make_noii(stock="AAPL")))
        size_b = len(_serialize_noii(_make_noii(stock="X", far_price=None, near_price=None)))
        assert size_a == size_b


# ---------------------------------------------------------------------------
# NULL price handling (NaN in binary → None in Postgres)
# ---------------------------------------------------------------------------

class TestNoiiNullPrices(unittest.TestCase):

    def test_none_far_price_becomes_nan_on_wire(self):
        """None far_price is encoded as NaN in the binary payload."""
        noii = _make_noii(far_price=None)
        result = deserialize_noii(_serialize_noii(noii))
        assert math.isnan(result["far_price"])

    def test_none_near_price_becomes_nan_on_wire(self):
        noii = _make_noii(near_price=None)
        result = deserialize_noii(_serialize_noii(noii))
        assert math.isnan(result["near_price"])

    def test_nan_to_none_converts_for_inserter(self):
        """DbInserter calls _nan_to_none before writing; verify the conversion."""
        noii = _make_noii(far_price=None, near_price=None)
        result = deserialize_noii(_serialize_noii(noii))

        assert _nan_to_none(result["far_price"]) is None
        assert _nan_to_none(result["near_price"]) is None

    def test_valid_price_survives_nan_to_none(self):
        """Real float prices must pass through _nan_to_none unchanged."""
        noii = _make_noii(far_price=150.25, near_price=150.00)
        result = deserialize_noii(_serialize_noii(noii))

        assert _nan_to_none(result["far_price"]) == pytest.approx(150.25)
        assert _nan_to_none(result["near_price"]) == pytest.approx(150.00)

    def test_both_null_prices_roundtrip(self):
        """Both prices None is the common case during opening cross early messages."""
        noii = _make_noii(far_price=None, near_price=None, cross_type="O")
        result = deserialize_noii(_serialize_noii(noii))
        assert _nan_to_none(result["far_price"]) is None
        assert _nan_to_none(result["near_price"]) is None
        assert result["current_reference_price"] == pytest.approx(182.00)


# ---------------------------------------------------------------------------
# itch_runner --publish flag logic
# ---------------------------------------------------------------------------

class TestPublishSetLogic(unittest.TestCase):
    """
    Test the publish_set expansion logic copied from itch_runner.main().
    Keeps itch_runner testable without importing the full module (which has
    heavy side-effect imports for Kafka/chart servers).
    """

    _ALL = {"trades", "tob", "vwap", "noii"}

    def _expand(self, publish_args: list[str]) -> set[str]:
        return self._ALL if "all" in publish_args else set(publish_args)

    def test_all_expands_to_four_publishers(self):
        assert self._expand(["all"]) == self._ALL

    def test_single_noii(self):
        result = self._expand(["noii"])
        assert result == {"noii"}
        assert "trades" not in result
        assert "tob" not in result

    def test_single_trades(self):
        assert self._expand(["trades"]) == {"trades"}

    def test_trades_and_tob(self):
        assert self._expand(["trades", "tob"]) == {"trades", "tob"}

    def test_explicit_all_four_same_as_all_keyword(self):
        explicit = self._expand(["trades", "tob", "vwap", "noii"])
        via_all = self._expand(["all"])
        assert explicit == via_all

    def test_all_with_other_keyword_still_gives_full_set(self):
        """'all' anywhere in the list expands to everything."""
        assert self._expand(["noii", "all"]) == self._ALL


# ---------------------------------------------------------------------------
# itch_runner --publish argparse wiring
# ---------------------------------------------------------------------------

class TestPublishArgparse(unittest.TestCase):
    """Verify the --publish argument is wired correctly in itch_runner."""

    def test_help_includes_publish_flag(self):
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "-m", "itch.itch_runner", "--help"],
            capture_output=True, text=True,
            env={**__import__("os").environ, "PYTHONPATH": "src"},
            cwd="/home/user/mpa",
        )
        assert "--publish" in result.stdout
        assert "noii" in result.stdout
        assert "trades" in result.stdout
