from pathlib import Path

from fastapi.testclient import TestClient

from src.service.app import app
from src.train import run_training


def ensure_demo_artifacts():
    artifacts_dir = Path("artifacts")
    if (artifacts_dir / "model.joblib").exists():
        return
    run_training(
        {
            "data_path": "data/hotel_bookings.csv",
            "target_column": "is_canceled",
            "test_size": 0.25,
            "random_state": 42,
            "artifacts_dir": "artifacts",
        }
    )


def test_health_endpoint():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()


def test_predict_endpoint():
    ensure_demo_artifacts()
    client = TestClient(app)
    response = client.post(
        "/predict",
        json={
            "lead_time": 100,
            "adr": 90.0,
            "adults": 2,
            "children": 0,
            "babies": 0,
            "previous_cancellations": 1,
            "previous_bookings_not_canceled": 0,
            "booking_changes": 1,
            "days_in_waiting_list": 0,
            "required_car_parking_spaces": 0,
            "total_of_special_requests": 1,
            "stays_in_weekend_nights": 1,
            "stays_in_week_nights": 3,
            "hotel": "City Hotel",
            "meal": "BB",
            "market_segment": "Online TA",
            "distribution_channel": "TA/TO",
            "deposit_type": "No Deposit",
            "customer_type": "Transient",
            "reserved_room_type": "A",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "cancel_probability" in payload
    assert payload["risk_level"] in {"low", "medium", "high"}
