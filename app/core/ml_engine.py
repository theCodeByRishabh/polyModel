from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier

from app.core.feature_engine import FeatureVector

logger = logging.getLogger(__name__)


class MLEngine:
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
    ]

    def __init__(self, model_path: Path):
        self.model_path = model_path
        self.model: Any | None = None
        self.params = {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "max_depth": 4,
            "min_child_weight": 8,
            "gamma": 1.5,
            "reg_lambda": 2.0,
            "reg_alpha": 1.0,
            "subsample": 0.7,
            "colsample_bytree": 0.7,
            "learning_rate": 0.05,
            "n_estimators": 150,
            "scale_pos_weight": 1.2,
            "random_state": 42,
        }

    def load(self) -> None:
        if not self.model_path.exists():
            logger.warning("No ML model found at %s. Starting without pre-trained model.", self.model_path)
            return
        try:
            payload = joblib.load(self.model_path)
            self.model = payload["model"] if isinstance(payload, dict) else payload
            logger.info("Loaded ML model from %s", self.model_path)
        except Exception as exc:
            logger.exception("Failed to load ML model: %s", exc)
            self.model = None

    def decide(self, features: FeatureVector) -> tuple[bool, float, float]:
        if self.model is None:
            return False, 0.5, 0.0
        row = pd.DataFrame([features.ml_row()], columns=self.feature_columns)
        prob = float(self.model.predict_proba(row)[0][1])
        expected_value = prob - features.price
        decision = bool(prob > 0.8 and expected_value > 0.0)
        return decision, prob, expected_value

    def train(self, training_rows: list[dict[str, Any]], window_size: int = 5000) -> bool:
        if len(training_rows) < 600:
            logger.info("Skipping ML train: need >= 600 resolved rows, got %d", len(training_rows))
            return False

        df = pd.DataFrame(training_rows)
        if "outcome" not in df.columns:
            logger.warning("Skipping ML train: missing outcome column")
            return False

        df = df.tail(window_size).copy()
        df["regime_volatile"] = (df["regime"] == "volatile").astype(float)
        if "price_time" not in df.columns:
            df["price_time"] = df["price"] * df["time_left"]
        if "gap_velocity" not in df.columns:
            df["gap_velocity"] = df["btc_gap"] * df["btc_velocity"]

        if df["outcome"].nunique() < 2:
            logger.info("Skipping ML train: outcome has one class only")
            return False

        split_index = max(1, len(df) - 500)
        train_df = df.iloc[:split_index]
        val_df = df.iloc[split_index:]
        if train_df.empty or val_df.empty:
            logger.info("Skipping ML train: not enough rows after time split")
            return False

        X_train = train_df[self.feature_columns]
        y_train = train_df["outcome"].astype(int)
        X_val = val_df[self.feature_columns]
        y_val = val_df["outcome"].astype(int)

        if y_train.sum() > 0:
            negatives = len(y_train) - int(y_train.sum())
            positives = int(y_train.sum())
            dynamic_spw = max(1.0, negatives / max(positives, 1))
        else:
            dynamic_spw = self.params["scale_pos_weight"]

        model_params = dict(self.params)
        model_params["scale_pos_weight"] = float(dynamic_spw)

        base_model = XGBClassifier(**model_params)
        base_model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

        calibrated_model = base_model
        if y_val.nunique() == 2:
            try:
                calibrator = CalibratedClassifierCV(estimator=base_model, method="sigmoid", cv="prefit")
            except TypeError:
                calibrator = CalibratedClassifierCV(base_estimator=base_model, method="sigmoid", cv="prefit")
            calibrator.fit(X_val, y_val)
            calibrated_model = calibrator
            logger.info("Applied probability calibration using holdout window.")
        else:
            logger.info("Skipped calibration due to single-class validation window.")

        self.model = calibrated_model
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "features": self.feature_columns,
                "window_size": window_size,
            },
            self.model_path,
        )
        logger.info("Trained and saved ML model at %s", self.model_path)
        return True
