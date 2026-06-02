from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .artifacts import load_joblib, load_json
from .features import FEATURE_COLUMNS


def probability_to_risk(probability: float) -> str:
    if probability < 0.35:
        return "low"
    if probability < 0.7:
        return "medium"
    return "high"


@dataclass
class PredictionArtifacts:
    model: object
    feature_columns: list[str]
    metadata: dict


def load_prediction_artifacts(artifacts_dir: str | Path) -> PredictionArtifacts:
    artifacts_path = Path(artifacts_dir)
    model = load_joblib(artifacts_path / "model.joblib")
    feature_columns = load_json(artifacts_path / "feature_columns.json")
    metadata = load_json(artifacts_path / "metadata.json")
    return PredictionArtifacts(
        model=model,
        feature_columns=feature_columns,
        metadata=metadata,
    )


def predict_single(artifacts: PredictionArtifacts, payload: dict) -> dict:
    row = {column: payload[column] for column in FEATURE_COLUMNS}
    features = pd.DataFrame([row], columns=artifacts.feature_columns)
    probability = float(artifacts.model.predict_proba(features)[0, 1])
    return {
        "cancel_probability": round(probability, 4),
        "risk_level": probability_to_risk(probability),
        "model_name": artifacts.metadata["best_model"],
    }
