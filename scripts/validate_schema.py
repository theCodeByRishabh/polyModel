from __future__ import annotations

from app.db.models import AggregatedStat, Metric, Observation


def assert_columns(model, required: set[str]) -> tuple[bool, set[str]]:
    actual = {col.name for col in model.__table__.columns}
    missing = required - actual
    return len(missing) == 0, missing


def main() -> int:
    observation_required = {
        "id",
        "timestamp",
        "market_slug",
        "price",
        "spread",
        "btc_reference_price",
        "btc_gap",
        "btc_velocity",
        "btc_volatility",
        "momentum_score",
        "price_change_rate",
        "orderbook_imbalance",
        "time_left",
        "time_bucket",
        "regime",
        "price_bucket",
        "btc_gap_bucket",
        "price_time",
        "gap_velocity",
        "decision_rule",
        "decision_ml",
        "final_decision",
        "prob_ml",
        "prob_meta",
        "expected_value",
        "meta_choice",
        "data_source",
        "outcome",
        "resolved",
    }
    aggregate_required = {
        "id",
        "bucket_day",
        "price_bucket",
        "time_bucket",
        "btc_gap_bucket",
        "count",
        "wins",
        "losses",
        "updated_at",
    }
    metric_required = {
        "id",
        "total_trades",
        "win_rate_rule",
        "win_rate_ml",
        "total_profit",
        "total_loss",
        "ev",
        "max_drawdown",
        "meta_accuracy",
        "ml_accuracy",
        "rule_accuracy",
        "meta_decision_distribution",
        "updated_at",
    }

    checks = [
        ("observations", Observation, observation_required),
        ("aggregated_stats", AggregatedStat, aggregate_required),
        ("metrics", Metric, metric_required),
    ]

    overall_ok = True
    for name, model, required in checks:
        ok, missing = assert_columns(model, required)
        if ok:
            print(f"[PASS] {name}: schema contains required columns ({len(required)})")
        else:
            overall_ok = False
            print(f"[FAIL] {name}: missing columns -> {sorted(missing)}")

    if overall_ok:
        print("Schema check complete: models are aligned with the new Polymarket-only data flow.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
