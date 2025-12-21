from __future__ import annotations

import json
import logging
import uuid
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field

from .core import compute_quality_flags, missing_table, summarize_dataset

# Context variable для хранения request_id
request_id_var: ContextVar[str] = ContextVar("request_id", default="unknown")

# ---------- Настройка структурированного логирования ----------


def setup_structured_logging() -> logging.Logger:
    """Настройка структурированного логирования в JSON формате."""
    # Создаём директорию для логов
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Настраиваем logger
    logger = logging.getLogger("api")
    logger.setLevel(logging.INFO)

    # Удаляем существующие handlers, чтобы избежать дублирования
    logger.handlers.clear()

    # Handler для записи в файл
    file_handler = logging.FileHandler(log_dir / "api.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    # Handler для записи в stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Форматтер для JSON
    class JSONFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            log_data = {
                "endpoint": getattr(record, "endpoint", "unknown"),
                "status": getattr(record, "status", "unknown"),
                "latency_ms": getattr(record, "latency_ms", 0.0),
                "ok_for_model": getattr(record, "ok_for_model", None),
                "n_rows": getattr(record, "n_rows", None),
                "n_cols": getattr(record, "n_cols", None),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "request_id": getattr(record, "request_id", "unknown"),
            }
            # Удаляем None значения для чистоты JSON
            log_data = {k: v for k, v in log_data.items() if v is not None}
            return json.dumps(log_data, ensure_ascii=False)

    json_formatter = JSONFormatter()
    file_handler.setFormatter(json_formatter)
    console_handler.setFormatter(json_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Инициализируем logger
api_logger = setup_structured_logging()


# ---------- Middleware для генерации request_id ----------


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Middleware для генерации уникального request_id для каждого запроса."""

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        # Сохраняем request_id в context variable для доступа из эндпоинтов
        token = request_id_var.set(request_id)
        try:
            response = await call_next(request)
            return response
        finally:
            request_id_var.reset(token)


app = FastAPI(
    title="AIE Dataset Quality API",
    version="0.2.0",
    description=(
        "HTTP-сервис-заглушка для оценки готовности датасета к обучению модели. "
        "Использует простые эвристики качества данных вместо настоящей ML-модели."
    ),
    docs_url="/docs",
    redoc_url=None,
)

# Добавляем middleware для request_id
app.add_middleware(RequestIDMiddleware)


# ---------- Модели запросов/ответов ----------


class QualityRequest(BaseModel):
    """Агрегированные признаки датасета – 'фичи' для заглушки модели."""

    n_rows: int = Field(..., ge=0, description="Число строк в датасете")
    n_cols: int = Field(..., ge=0, description="Число колонок")
    max_missing_share: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Максимальная доля пропусков среди всех колонок (0..1)",
    )
    numeric_cols: int = Field(
        ...,
        ge=0,
        description="Количество числовых колонок",
    )
    categorical_cols: int = Field(
        ...,
        ge=0,
        description="Количество категориальных колонок",
    )


class QualityResponse(BaseModel):
    """Ответ заглушки модели качества датасета."""

    ok_for_model: bool = Field(
        ...,
        description="True, если датасет считается достаточно качественным для обучения модели",
    )
    quality_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Интегральная оценка качества данных (0..1)",
    )
    message: str = Field(
        ...,
        description="Человекочитаемое пояснение решения",
    )
    latency_ms: float = Field(
        ...,
        ge=0.0,
        description="Время обработки запроса на сервере, миллисекунды",
    )
    flags: dict[str, bool] | None = Field(
        default=None,
        description="Булевы флаги с подробностями (например, too_few_rows, too_many_missing)",
    )
    dataset_shape: dict[str, int] | None = Field(
        default=None,
        description="Размеры датасета: {'n_rows': ..., 'n_cols': ...}, если известны",
    )


class QualityFlagsResponse(BaseModel):
    """Ответ с полным набором флагов качества данных."""

    flags: dict[str, bool] = Field(
        ...,
        description="Полный набор булевых флагов качества данных, включая новые эвристики из HW03",
    )


# ---------- Функция для структурированного логирования ----------


def log_request(
    endpoint: str,
    status: str,
    latency_ms: float,
    ok_for_model: bool | None = None,
    n_rows: int | None = None,
    n_cols: int | None = None,
) -> None:
    """Логирует запрос в структурированном JSON формате."""
    request_id = request_id_var.get("unknown")
    log_record = logging.LogRecord(
        name="api",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    )
    log_record.endpoint = endpoint
    log_record.status = status
    log_record.latency_ms = latency_ms
    log_record.ok_for_model = ok_for_model
    log_record.n_rows = n_rows
    log_record.n_cols = n_cols
    log_record.request_id = request_id

    api_logger.handle(log_record)


# ---------- Системный эндпоинт ----------


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    """Простейший health-check сервиса."""
    return {
        "status": "ok",
        "service": "dataset-quality",
        "version": "0.2.0",
    }


# ---------- Заглушка /quality по агрегированным признакам ----------


@app.post("/quality", response_model=QualityResponse, tags=["quality"])
def quality(req: QualityRequest) -> QualityResponse:
    """
    Эндпоинт-заглушка, который принимает агрегированные признаки датасета
    и возвращает эвристическую оценку качества.
    """

    start = perf_counter()

    # Базовый скор от 0 до 1
    score = 1.0

    # Чем больше пропусков, тем хуже
    score -= req.max_missing_share

    # Штраф за слишком маленький датасет
    if req.n_rows < 1000:
        score -= 0.2

    # Штраф за слишком широкий датасет
    if req.n_cols > 100:
        score -= 0.1

    # Штрафы за перекос по типам признаков (если есть числовые и категориальные)
    if req.numeric_cols == 0 and req.categorical_cols > 0:
        score -= 0.1
    if req.categorical_cols == 0 and req.numeric_cols > 0:
        score -= 0.05

    # Нормируем скор в диапазон [0, 1]
    score = max(0.0, min(1.0, score))

    # Простое решение "ок / не ок"
    ok_for_model = score >= 0.7
    if ok_for_model:
        message = "Данных достаточно, модель можно обучать (по текущим эвристикам)."
    else:
        message = "Качество данных недостаточно, требуется доработка (по текущим эвристикам)."

    latency_ms = (perf_counter() - start) * 1000.0

    # Флаги, которые могут быть полезны для последующего логирования/аналитики
    flags = {
        "too_few_rows": req.n_rows < 1000,
        "too_many_columns": req.n_cols > 100,
        "too_many_missing": req.max_missing_share > 0.5,
        "no_numeric_columns": req.numeric_cols == 0,
        "no_categorical_columns": req.categorical_cols == 0,
    }

    # Структурированное логирование
    log_request(
        endpoint="/quality",
        status="success",
        latency_ms=latency_ms,
        ok_for_model=ok_for_model,
        n_rows=req.n_rows,
        n_cols=req.n_cols,
    )

    return QualityResponse(
        ok_for_model=ok_for_model,
        quality_score=score,
        message=message,
        latency_ms=latency_ms,
        flags=flags,
        dataset_shape={"n_rows": req.n_rows, "n_cols": req.n_cols},
    )


# ---------- /quality-from-csv: реальный CSV через нашу EDA-логику ----------


@app.post(
    "/quality-from-csv",
    response_model=QualityResponse,
    tags=["quality"],
    summary="Оценка качества по CSV-файлу с использованием EDA-ядра",
)
async def quality_from_csv(file: UploadFile = File(...)) -> QualityResponse:
    """
    Эндпоинт, который принимает CSV-файл, запускает EDA-ядро
    (summarize_dataset + missing_table + compute_quality_flags)
    и возвращает оценку качества данных.

    Именно это по сути связывает S03 (CLI EDA) и S04 (HTTP-сервис).
    """

    start = perf_counter()

    try:
        if file.content_type not in ("text/csv", "application/vnd.ms-excel", "application/octet-stream"):
            # content_type от браузера может быть разным, поэтому проверка мягкая
            # но для демонстрации оставим простую ветку 400
            latency_ms = (perf_counter() - start) * 1000.0
            log_request(
                endpoint="/quality-from-csv",
                status="error",
                latency_ms=latency_ms,
            )
            raise HTTPException(status_code=400, detail="Ожидается CSV-файл (content-type text/csv).")

        # FastAPI даёт file.file как file-like объект, который можно читать pandas'ом
        df = pd.read_csv(file.file)

        if df.empty:
            latency_ms = (perf_counter() - start) * 1000.0
            log_request(
                endpoint="/quality-from-csv",
                status="error",
                latency_ms=latency_ms,
            )
            raise HTTPException(status_code=400, detail="CSV-файл не содержит данных (пустой DataFrame).")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        latency_ms = (perf_counter() - start) * 1000.0
        log_request(
            endpoint="/quality-from-csv",
            status="error",
            latency_ms=latency_ms,
        )
        raise HTTPException(status_code=400, detail=f"Не удалось прочитать CSV: {exc}")

    # Используем EDA-ядро из S03
    summary = summarize_dataset(df)
    missing_df = missing_table(df)
    flags_all = compute_quality_flags(summary, missing_df, df)

    # Ожидаем, что compute_quality_flags вернёт quality_score в [0,1]
    score = float(flags_all.get("quality_score", 0.0))
    score = max(0.0, min(1.0, score))
    ok_for_model = score >= 0.7

    if ok_for_model:
        message = "CSV выглядит достаточно качественным для обучения модели (по текущим эвристикам)."
    else:
        message = "CSV требует доработки перед обучением модели (по текущим эвристикам)."

    latency_ms = (perf_counter() - start) * 1000.0

    # Оставляем только булевы флаги для компактности
    flags_bool: dict[str, bool] = {
        key: bool(value)
        for key, value in flags_all.items()
        if isinstance(value, bool)
    }

    # Размеры датасета берём из summary (если там есть поля n_rows/n_cols),
    # иначе — напрямую из DataFrame.
    try:
        n_rows = int(getattr(summary, "n_rows"))
        n_cols = int(getattr(summary, "n_cols"))
    except AttributeError:
        n_rows = int(df.shape[0])
        n_cols = int(df.shape[1])

    # Структурированное логирование
    log_request(
        endpoint="/quality-from-csv",
        status="success",
        latency_ms=latency_ms,
        ok_for_model=ok_for_model,
        n_rows=n_rows,
        n_cols=n_cols,
    )

    return QualityResponse(
        ok_for_model=ok_for_model,
        quality_score=score,
        message=message,
        latency_ms=latency_ms,
        flags=flags_bool,
        dataset_shape={"n_rows": n_rows, "n_cols": n_cols},
    )


# ---------- /quality-flags-from-csv: полный набор флагов качества из CSV ----------


@app.post(
    "/quality-flags-from-csv",
    response_model=QualityFlagsResponse,
    tags=["quality"],
    summary="Полный набор флагов качества по CSV-файлу с использованием EDA-ядра",
)
async def quality_flags_from_csv(file: UploadFile = File(...)) -> QualityFlagsResponse:
    """
    Эндпоинт, который принимает CSV-файл, запускает EDA-ядро
    (summarize_dataset + missing_table + compute_quality_flags)
    и возвращает полный набор булевых флагов качества данных,
    включая новые эвристики из HW03:
    - has_constant_columns
    - has_high_cardinality_categoricals
    - high_duplicate_values_ratio
    - has_many_zero_values
    и другие базовые флаги.
    """

    start = perf_counter()

    try:
        if file.content_type not in ("text/csv", "application/vnd.ms-excel", "application/octet-stream"):
            latency_ms = (perf_counter() - start) * 1000.0
            log_request(
                endpoint="/quality-flags-from-csv",
                status="error",
                latency_ms=latency_ms,
            )
            raise HTTPException(status_code=400, detail="Ожидается CSV-файл (content-type text/csv).")

        df = pd.read_csv(file.file)

        if df.empty:
            latency_ms = (perf_counter() - start) * 1000.0
            log_request(
                endpoint="/quality-flags-from-csv",
                status="error",
                latency_ms=latency_ms,
            )
            raise HTTPException(status_code=400, detail="CSV-файл не содержит данных (пустой DataFrame).")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        latency_ms = (perf_counter() - start) * 1000.0
        log_request(
            endpoint="/quality-flags-from-csv",
            status="error",
            latency_ms=latency_ms,
        )
        raise HTTPException(status_code=400, detail=f"Не удалось прочитать CSV: {exc}")

    # Используем EDA-ядро из S03, передаём df для вычисления всех флагов
    summary = summarize_dataset(df)
    missing_df = missing_table(df)
    flags_all = compute_quality_flags(summary, missing_df, df)

    latency_ms = (perf_counter() - start) * 1000.0

    # Оставляем только булевы флаги (исключаем quality_score и max_missing_share)
    flags_bool: dict[str, bool] = {
        key: bool(value)
        for key, value in flags_all.items()
        if isinstance(value, bool)
    }

    # Структурированное логирование
    log_request(
        endpoint="/quality-flags-from-csv",
        status="success",
        latency_ms=latency_ms,
        ok_for_model=None,  # Этот эндпоинт не возвращает ok_for_model
        n_rows=summary.n_rows,
        n_cols=summary.n_cols,
    )

    return QualityFlagsResponse(flags=flags_bool)
