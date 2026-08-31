from __future__ import annotations

import json
from pathlib import Path

import pytest

from viskium.storage.layout import (
    DATA_CATEGORIES,
    DATA_ROOT_MARKER,
    StorageLayoutError,
    initialize_data_root,
    verify_data_root,
)


def test_initialize_publishes_a_complete_versioned_layout(tmp_path: Path) -> None:
    root = tmp_path / "data"

    layout = initialize_data_root(root, root_id="12345678-1234-5678-1234-567812345678")

    assert layout.root == root
    assert layout.root_id == "12345678-1234-5678-1234-567812345678"
    assert all(layout.category(name).is_dir() for name in DATA_CATEGORIES)
    marker = json.loads((root / DATA_ROOT_MARKER).read_text(encoding="utf-8"))
    assert marker == {
        "categories": list(DATA_CATEGORIES),
        "kind": "viskium.data-root",
        "root_id": layout.root_id,
        "schema_version": 1,
    }
    assert not tuple(root.glob(f".{DATA_ROOT_MARKER}.*.tmp"))
    assert verify_data_root(root) == layout


def test_initialization_is_idempotent_only_for_a_valid_marked_root(tmp_path: Path) -> None:
    root = tmp_path / "data"
    first = initialize_data_root(root)

    assert initialize_data_root(root) == first

    unmarked = tmp_path / "unmarked"
    unmarked.mkdir()
    (unmarked / "foreign.txt").write_text("not owned", encoding="utf-8")
    with pytest.raises(StorageLayoutError, match="must be empty"):
        initialize_data_root(unmarked)


def test_verification_never_repairs_missing_or_tampered_content(tmp_path: Path) -> None:
    root = tmp_path / "data"
    initialize_data_root(root)
    missing = root / DATA_CATEGORIES[0]
    missing.rmdir()

    with pytest.raises(StorageLayoutError, match="layout path"):
        verify_data_root(root)
    assert not missing.exists()

    missing.mkdir()
    marker_path = root / DATA_ROOT_MARKER
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["categories"] = ["observations"]
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    with pytest.raises(StorageLayoutError, match="categories"):
        verify_data_root(root)


def test_category_names_and_dangerous_roots_are_rejected(tmp_path: Path) -> None:
    layout = initialize_data_root(tmp_path / "data")
    with pytest.raises(StorageLayoutError, match="unsupported"):
        layout.category("../outside")
    with pytest.raises(StorageLayoutError, match="filesystem root"):
        initialize_data_root(Path(tmp_path.anchor))
