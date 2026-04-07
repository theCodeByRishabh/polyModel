from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from xgboost import XGBClassifier

from app.core.feature_engine import FeatureVector

logger = logging.getLogger(__name__)


class MetaEngine:
    feature_columns = [
        "price",
        "spread",
        "btc_gap",
        "btc_velocity",
        "btc_volatility",
        "momentum_score",
        "price_change_rate",
        "orderbook_imbalance",
        "time_left",
        "regime_volatile",
        "price_time",
        "gap_velocity",
        "prob_ml",
        "decision_rule",
        "decision_ml",
    ]

    def __init__(self, model_path: Path):
        self.model_path = model_path
        self.model: Any | None = None
        self.params = {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "max_depth": 3,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        }

    def load(self) -> None:
        if not self.model_path.exists():
            logger.warning("No meta model found at %s. Starting with heuristic fallback.", self.model_path)
            return
        try:
            payload = joblib.load(self.model_path)
            self.model = payload["model"] if isinstance(payload, dict) else payload
            logger.info("Loaded meta model from %s", self.model_path)
        except Exception as exc:
            logger.exception("Failed to load meta model: %s", exc)
            self.model = None

    def choose(
        self,
        *,
        features: FeatureVector,
        decision_rule: bool,
        decision_ml: bool,
        prob_ml: float,
    ) -> tuple[str, float]:
        if self.model is None:
            if prob_ml >= 0.85:
                return "ml", 0.8
            if features.price >= 0.97:
                return "rule", 0.2
            return "skip", 0.5

        row = pd.DataFrame(
            [features.meta_row(decision_rule=decision_rule, decision_ml=decision_ml, prob_ml=prob_ml)],
            columns=self.feature_columns,
        )
        prob_meta = float(self.model.predict_proba(row)[0][1])
        if prob_meta > 0.7:
            return "ml", prob_meta
        if prob_meta < 0.3:
            return "rule", prob_meta
        return "skip", prob_meta

    def train(self, training_rows: list[dict[str, Any]], min_rows: int = 200) -> bool:
        if len(training_rows) < min_rows:
            logger.info("Skipping meta train: need >= %d resolved rows, got %d", min_rows, len(training_rows))
            return False

        df = pd.DataFrame(training_rows).copy()
        required = {"decision_rule", "decision_ml", "outcome", "prob_ml"}
        if not required.issubset(df.columns):
            logger.warning("Skipping meta train: missing columns %s", required - set(df.columns))
            return False

        df["rule_correct"] = (df["decision_rule"].astype(bool) == df["outcome"].astype(bool)).astype(int)
        df["ml_correct"] = (df["decision_ml"].astype(bool) == df["outcome"].astype(bool)).astype(int)

        only_rule = (df["rule_correct"] == 1) & (df["ml_correct"] == 0)
        only_ml = (df["rule_correct"] == 0) & (df["ml_correct"] == 1)
        signal_df = df[only_rule | only_ml].copy()
        if signal_df.empty or len(signal_df) < min_rows:
            logger.info(
                "Skipping meta train: not enough disagreement rows (need %d, got %d)",
                min_rows,
                len(signal_df),
            )
            return False

        signal_df["meta_target"] = only_ml[signal_df.index].astype(int)
        signal_df["regime_volatile"] = (signal_df["regime"] == "volatile").astype(float)
        if "price_time" not in signal_df.columns:
            signal_df["price_time"] = signal_df["price"] * signal_df["time_left"]
        if "gap_velocity" not in signal_df.columns:
            signal_df["gap_velocity"] = signal_df["btc_gap"] * signal_df["btc_velocity"]
        signal_df["decision_rule"] = signal_df["decision_rule"].astype(float)
        signal_df["decision_ml"] = signal_df["decision_ml"].astype(float)

        if signal_df["meta_target"].nunique() < 2:
            logger.info("Skipping meta train: target class is single-value")
            return False

        split_index = max(1, len(signal_df) - 100)
        train_df = signal_df.iloc[:split_index]
        val_df = signal_df.iloc[split_index:]
        if train_df.empty or val_df.empty:
            logger.info("Skipping meta train: not enough rows after time split")
            return False

        X_train = train_df[self.feature_columns]
        y_train = train_df["meta_target"].astype(int)
        X_val = val_df[self.feature_columns]
        y_val = val_df["meta_target"].astype(int)

        model = XGBClassifier(**self.params)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
        self.model = model

        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"model": self.model, "features": self.feature_columns}, self.model_path)
        logger.info("Trained and saved meta model at %s", self.model_path)
        return True
