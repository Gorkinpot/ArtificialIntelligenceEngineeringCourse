import json
from pathlib import Path

from src.train import run_training


def test_training_creates_artifacts(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    config = {
        "data_path": "data/hotel_bookings.csv",
        "target_column": "is_canceled",
        "test_size": 0.25,
        "random_state": 42,
        "artifacts_dir": str(artifacts_dir),
    }

    result = run_training(config)

    assert result["best_model"]
    assert (artifacts_dir / "model.joblib").exists()
    assert (artifacts_dir / "metrics.json").exists()

    metrics = json.loads((artifacts_dir / "metrics.json").read_text(encoding="utf-8"))
    assert "logistic_regression" in metrics
