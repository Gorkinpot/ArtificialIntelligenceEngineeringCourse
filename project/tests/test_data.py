from pathlib import Path

from src.data import load_dataset, split_features_target


def test_load_dataset():
    df = load_dataset(Path("data/hotel_bookings.csv"))
    x, y = split_features_target(df)

    assert not df.empty
    assert x.shape[0] == y.shape[0]
    assert "hotel" in x.columns
