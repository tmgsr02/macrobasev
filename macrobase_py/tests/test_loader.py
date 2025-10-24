import pandas as pd
import pytest

from macrobase_py.data.loader import ColumnMetadata, DataLoader


def test_csv_ingestion_and_schema_detection(tmp_path):
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "value": ["10.5", "20.1", None, "42.0"],
            "category": ["A", "A", "B", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="D"),
            "flag": [True, False, True, None],
        }
    )
    csv_path = tmp_path / "sample.csv"
    df.to_csv(csv_path, index=False)

    loader = DataLoader(csv_path)

    loaded = loader.load()
    assert loaded["value"].dtype.kind in {"f", "i"}
    assert loaded["timestamp"].dtype.kind == "M"
    assert loaded["category"].dtype.name == "category"

    schema = loader.schema
    assert isinstance(schema["value"], ColumnMetadata)
    assert schema["value"].logical_type == "numeric"
    assert schema["category"].categorical is True
    assert schema["flag"].logical_type == "boolean"
    assert schema["value"].null_count == 1


def test_chunked_iteration_applies_schema(tmp_path):
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "value": ["1", "2", "3", "4"],
        }
    )
    csv_path = tmp_path / "chunked.csv"
    df.to_csv(csv_path, index=False)

    loader = DataLoader(csv_path, chunk_size=2)

    batches = list(loader.iter_batches())
    assert len(batches) == 2
    assert all(batch["value"].dtype.kind in {"i", "f"} for batch in batches)


def test_excel_ingestion(tmp_path):
    pytest.importorskip("openpyxl")

    df = pd.DataFrame(
        {
            "name": ["alice", "bob"],
            "score": [10, 20],
        }
    )
    excel_path = tmp_path / "sample.xlsx"
    try:
        df.to_excel(excel_path, index=False)
    except ModuleNotFoundError:
        pytest.skip("openpyxl is required for Excel tests")

    loader = DataLoader(excel_path)
    loaded = loader.load()

    assert loaded.equals(df)
    assert loader.schema["name"].logical_type == "categorical"
    assert loader.schema["score"].logical_type == "numeric"


def test_schema_overrides(tmp_path):
    df = pd.DataFrame({"text": ["1", "2"]})
    csv_path = tmp_path / "override.csv"
    df.to_csv(csv_path, index=False)

    loader = DataLoader(
        csv_path,
        schema_overrides={"text": {"logical_type": "text", "categorical": False}},
    )

    assert loader.schema["text"].logical_type == "text"
    assert loader.schema["text"].categorical is False


def test_invalid_chunking_for_excel(tmp_path):
    excel_path = tmp_path / "sample.xlsx"
    try:
        pd.DataFrame({"a": [1]}).to_excel(excel_path, index=False)
    except ModuleNotFoundError:
        pytest.skip("openpyxl is required for Excel tests")

    with pytest.raises(ValueError):
        DataLoader(excel_path, chunk_size=1)


def test_unknown_file_type(tmp_path):
    path = tmp_path / "data.unknown"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError):
        DataLoader(path)
