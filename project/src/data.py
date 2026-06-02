from __future__ import annotations

from pathlib import Path

import pandas as pd

from .features import CATEGORICAL_FEATURES, FEATURE_COLUMNS, NUMERIC_FEATURES

TARGET_COLUMN = "is_canceled"


def load_dataset(path: str | Path) -> pd.DataFrame:
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    df = pd.read_csv(dataset_path, low_memory=False)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace("\ufeff", "", regex=False)
    for column in NUMERIC_FEATURES:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if TARGET_COLUMN in df.columns:
        df[TARGET_COLUMN] = pd.to_numeric(df[TARGET_COLUMN], errors="coerce")
    for column in CATEGORICAL_FEATURES:
        if column in df.columns:
            df[column] = df[column].astype("string")
    validate_dataset(df)
    return df


def validate_dataset(df: pd.DataFrame) -> None:
    required_columns = set(FEATURE_COLUMNS + [TARGET_COLUMN])
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise ValueError(f"Dataset is missing required columns: {missing_columns}")

    if df.empty:
        raise ValueError("Dataset is empty")

    unique_targets = set(df[TARGET_COLUMN].dropna().astype(int).unique().tolist())
    if not unique_targets.issubset({0, 1}):
        raise ValueError("Target column must contain only 0 and 1")


def split_features_target(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    x = df[FEATURE_COLUMNS].copy()
    y = df[TARGET_COLUMN].copy()
    return x, y
