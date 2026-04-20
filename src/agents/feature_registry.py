"""
Feature registry: maps feature names to their expected Kafka topics, Postgres tables,
ITCH runner flags, and SQL verification queries used by the test agent.

When adding a new feature:
1. Add an entry to FEATURES with topics, tables, and verify_queries.
2. verify_queries is a list of dicts with keys:
   - sql:         Parameterized SQL using %(date)s for the test trade_date.
   - description: Human-readable label shown in test output.
   - min_rows:    Minimum number of rows returned for the query to pass (0 = informational only).
"""

FEATURES: dict[str, dict] = {
    "trades": {
        "description": (
            "Non-cross trades (message type P) and order-execution trades (E/C). "
            "Published to Kafka 'trades' topic when --kafka is set."
        ),
        "topics": ["trades"],
        "tables": ["trades"],
        "verify_queries": [
            {
                "sql": "SELECT count(*) AS cnt FROM trades WHERE trade_date = %(date)s",
                "description": "Total trade rows",
                "min_rows": 1,
            },
            {
                "sql": (
                    "SELECT trade_type, count(*) AS cnt "
                    "FROM trades WHERE trade_date = %(date)s "
                    "GROUP BY trade_type ORDER BY trade_type"
                ),
                "description": "Trades by type (E=execution, N=non-cross, O=open cross, C=close cross)",
                "min_rows": 1,
            },
            {
                "sql": "SELECT count(DISTINCT sec_id) AS stocks FROM trades WHERE trade_date = %(date)s",
                "description": "Distinct securities with trades",
                "min_rows": 1,
            },
        ],
    },

    "tob": {
        "description": (
            "Top-of-book snapshots emitted on every order-book change. "
            "Published to Kafka 'tob' topic when --kafka is set."
        ),
        "topics": ["tob"],
        "tables": ["tob"],
        "verify_queries": [
            {
                "sql": "SELECT count(*) AS cnt FROM tob WHERE trade_date = %(date)s",
                "description": "Total TOB snapshot rows",
                "min_rows": 1,
            },
            {
                "sql": "SELECT count(DISTINCT stock) AS stocks FROM tob WHERE trade_date = %(date)s",
                "description": "Distinct securities with TOB updates",
                "min_rows": 1,
            },
        ],
    },

    "vwap": {
        "description": (
            "Rolling VWAP calculations at configurable time intervals. "
            "Published to Kafka 'vwap' topic when --kafka is set."
        ),
        "topics": ["vwap"],
        "tables": ["vwap"],
        "verify_queries": [
            {
                "sql": (
                    "SELECT interval_ms, count(*) AS cnt "
                    "FROM vwap WHERE trade_date = %(date)s "
                    "GROUP BY interval_ms ORDER BY interval_ms"
                ),
                "description": "VWAP rows by interval bucket (ms)",
                "min_rows": 1,
            },
            {
                "sql": (
                    "SELECT count(*) AS cnt FROM vwap "
                    "WHERE trade_date = %(date)s AND vwap_price IS NOT NULL"
                ),
                "description": "VWAP rows with a valid price (non-NULL)",
                "min_rows": 1,
            },
        ],
    },

    "noii": {
        "description": (
            "Net Order Imbalance Indicator (ITCH message type I). "
            "Emitted by NASDAQ during the opening cross (~9:25–9:30 ET) and "
            "closing cross (~3:50–4:00 ET). Published to Kafka 'noii' topic when --kafka is set. "
            "Requires processing enough messages to reach the cross windows. "
            "For a full trading day file, do NOT use --max-msgs or use a large enough limit."
        ),
        "topics": ["noii"],
        "tables": ["noii"],
        "verify_queries": [
            {
                "sql": "SELECT count(*) AS cnt FROM noii WHERE trade_date = %(date)s",
                "description": "Total NOII rows",
                "min_rows": 1,
            },
            {
                "sql": (
                    "SELECT cross_type, count(*) AS cnt "
                    "FROM noii WHERE trade_date = %(date)s "
                    "GROUP BY cross_type ORDER BY cross_type"
                ),
                "description": "NOII rows by cross type (O=opening, C=closing, H=IPO/halt, A=extended)",
                "min_rows": 1,
            },
            {
                "sql": (
                    "SELECT imbalance_direction, count(*) AS cnt "
                    "FROM noii WHERE trade_date = %(date)s "
                    "GROUP BY imbalance_direction ORDER BY imbalance_direction"
                ),
                "description": "NOII rows by imbalance direction (B=buy, S=sell, N=none, O=insufficient)",
                "min_rows": 1,
            },
            {
                "sql": (
                    "SELECT count(*) AS cnt FROM noii "
                    "WHERE trade_date = %(date)s "
                    "AND current_reference_price IS NOT NULL "
                    "AND current_reference_price > 0"
                ),
                "description": "NOII rows with a valid current reference price",
                "min_rows": 1,
            },
        ],
    },

    "all": {
        "description": (
            "Full pipeline: trades, TOB, VWAP, and NOII. "
            "All four Kafka topics are published when --kafka is set."
        ),
        "topics": ["trades", "tob", "vwap", "noii"],
        "tables": ["trades", "tob", "vwap", "noii"],
        "verify_queries": [
            {
                "sql": "SELECT count(*) AS cnt FROM trades WHERE trade_date = %(date)s",
                "description": "Trades",
                "min_rows": 1,
            },
            {
                "sql": "SELECT count(*) AS cnt FROM tob WHERE trade_date = %(date)s",
                "description": "TOB snapshots",
                "min_rows": 1,
            },
            {
                "sql": "SELECT count(*) AS cnt FROM vwap WHERE trade_date = %(date)s",
                "description": "VWAP rows",
                "min_rows": 1,
            },
            {
                "sql": "SELECT count(*) AS cnt FROM noii WHERE trade_date = %(date)s",
                "description": "NOII rows",
                "min_rows": 1,
            },
        ],
    },
}
