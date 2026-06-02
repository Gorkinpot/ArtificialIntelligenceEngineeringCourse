from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi import Request
from pydantic import BaseModel, Field

from ..features import FEATURE_COLUMNS
from ..inference import load_prediction_artifacts, predict_single
from ..logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "artifacts"))

app = FastAPI(title="Hotel Booking Cancellation Risk Service", version="0.1.0")


class PredictionRequest(BaseModel):
    lead_time: float = Field(..., ge=0)
    adr: float = Field(..., ge=0)
    adults: int = Field(..., ge=0)
    children: int = Field(..., ge=0)
    babies: int = Field(..., ge=0)
    previous_cancellations: int = Field(..., ge=0)
    previous_bookings_not_canceled: int = Field(..., ge=0)
    booking_changes: int = Field(..., ge=0)
    days_in_waiting_list: int = Field(..., ge=0)
    required_car_parking_spaces: int = Field(..., ge=0)
    total_of_special_requests: int = Field(..., ge=0)
    stays_in_weekend_nights: int = Field(..., ge=0)
    stays_in_week_nights: int = Field(..., ge=0)
    hotel: str
    meal: str
    market_segment: str
    distribution_channel: str
    deposit_type: str
    customer_type: str
    reserved_room_type: str


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "request method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


def get_artifacts():
    if not (ARTIFACTS_DIR / "model.joblib").exists():
        raise FileNotFoundError("Model artifacts not found. Run training first.")
    return load_prediction_artifacts(ARTIFACTS_DIR)


@app.get("/health")
def health() -> dict:
    artifacts_exist = (ARTIFACTS_DIR / "model.joblib").exists()
    return {
        "status": "ok" if artifacts_exist else "degraded",
        "artifacts_dir": str(ARTIFACTS_DIR),
        "model_ready": artifacts_exist,
    }


@app.get("/")
def root() -> dict:
    return {
        "message": "Hotel booking cancellation risk service",
        "features_count": len(FEATURE_COLUMNS),
    }


@app.post("/predict")
def predict(request: PredictionRequest) -> dict:
    try:
        artifacts = get_artifacts()
        payload = request.model_dump()
        result = predict_single(artifacts, payload)
        logger.info("Prediction served with risk=%s", result["risk_level"])
        return result
    except FileNotFoundError as exc:
        logger.exception("Artifacts are missing")
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Prediction failed")
        raise HTTPException(status_code=500, detail="Prediction failed") from exc
