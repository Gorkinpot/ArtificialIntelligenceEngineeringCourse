from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from .artifacts import ensure_dir, save_joblib, save_json
from .data import load_dataset, split_features_target
from .features import FEATURE_COLUMNS, build_preprocessor
from .logging_config import configure_logging
from .models import build_candidate_models

logger = logging.getLogger(__name__)


def load_config(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def evaluate_model(model: Pipeline, x_test, y_test) -> dict[str, float]:
    probabilities = model.predict_proba(x_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    return {
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "f1": float(f1_score(y_test, predictions)),
        "pr_auc": float(average_precision_score(y_test, probabilities)),
    }


def run_training(config: dict) -> dict:
    configure_logging()

    df = load_dataset(config["data_path"])
    x, y = split_features_target(df)

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=config.get("test_size", 0.25),
        random_state=config.get("random_state", 42),
        stratify=y,
    )

    preprocessor = build_preprocessor()
    models = build_candidate_models(config.get("random_state", 42))
    metrics_summary: dict[str, dict[str, float]] = {}
    trained_models: dict[str, Pipeline] = {}

    for model_name, estimator in models.items():
        logger.info("Training model: %s", model_name)
        pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", estimator),
            ]
        )
        pipeline.fit(x_train, y_train)
        metrics = evaluate_model(pipeline, x_test, y_test)
        metrics_summary[model_name] = metrics
        trained_models[model_name] = pipeline
        logger.info("Metrics for %s: %s", model_name, metrics)

    best_model_name = max(metrics_summary, key=lambda name: metrics_summary[name]["roc_auc"])
    best_model = trained_models[best_model_name]

    artifacts_dir = ensure_dir(config["artifacts_dir"])
    save_joblib(artifacts_dir / "model.joblib", best_model)
    save_joblib(artifacts_dir / "preprocessor.joblib", best_model.named_steps["preprocessor"])
    save_json(artifacts_dir / "metrics.json", metrics_summary)
    save_json(artifacts_dir / "feature_columns.json", FEATURE_COLUMNS)
    save_json(
        artifacts_dir / "metadata.json",
        {
            "best_model": best_model_name,
            "data_path": config["data_path"],
            "target_column": config["target_column"],
        },
    )
    return {
        "best_model": best_model_name,
        "metrics": metrics_summary,
        "artifacts_dir": str(artifacts_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train hotel booking cancellation model")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/train.json",
        help="Path to training config JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    result = run_training(config)
    logger.info("Training complete. Best model: %s", result["best_model"])


if __name__ == "__main__":
    main()
