from __future__ import annotations

import binascii
import struct
import zlib
from pathlib import Path
from typing import Any

import pytest

from viskium.core import FrameEnvelope
from viskium.snapshots import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    SnapshotEncodingError,
    SnapshotTooLargeError,
    encode_png_snapshot,
)
from viskium.snapshots import png as png_module
from viskium.snapshots.contracts import PNG_SIGNATURE


def _frame(**overrides: Any) -> FrameEnvelope:
    values: dict[str, Any] = {
        "source_id": "camera",
        "stream_epoch": 2,
        "sequence": 3,
        "received_monotonic_ns": 4,
        "width": 2,
        "height": 2,
        "stride": 2,
        "pixel_format": "gray8",
        "payload": bytes((1, 2, 3, 4)),
    }
    values.update(overrides)
    return FrameEnvelope(**values)


def _decode_png(png_bytes: bytes) -> tuple[int, int, int, bytes]:
    assert png_bytes.startswith(PNG_SIGNATURE)
    offset = len(PNG_SIGNATURE)
    width = height = color_type = -1
    compressed: list[bytes] = []
    while offset < len(png_bytes):
        length = struct.unpack(">I", png_bytes[offset : offset + 4])[0]
        chunk_type = png_bytes[offset + 4 : offset + 8]
        data = png_bytes[offset + 8 : offset + 8 + length]
        checksum = struct.unpack(">I", png_bytes[offset + 8 + length : offset + 12 + length])[0]
        assert binascii.crc32(chunk_type + data) & 0xFFFF_FFFF == checksum
        offset += 12 + length
        if chunk_type == b"IHDR":
            width, height, _depth, color_type, _compression, _filter, _interlace = struct.unpack(
                ">IIBBBBB", data
            )
        elif chunk_type == b"IDAT":
            compressed.append(data)
        elif chunk_type == b"IEND":
            break
    return width, height, color_type, zlib.decompress(b"".join(compressed))


def test_gray8_encoding_is_deterministic_and_preserves_metadata() -> None:
    frame = _frame()

    first = encode_png_snapshot(frame)
    second = encode_png_snapshot(frame)
    width, height, color_type, scanlines = _decode_png(first.png_bytes)

    assert first == second
    assert first.source_id == frame.source_id
    assert first.stream_epoch == frame.stream_epoch
    assert first.source_sequence == frame.sequence
    assert first.received_monotonic_ns == frame.received_monotonic_ns
    assert first.sensitivity_class == "identifiable"
    assert (width, height, color_type) == (2, 2, 0)
    assert scanlines == b"\x00\x01\x02\x00\x03\x04"
    assert first.encoded_bytes <= MAX_SNAPSHOT_BYTES


@pytest.mark.parametrize(
    ("pixel_format", "payload", "expected"),
    [
        ("rgb24", bytes((1, 2, 3, 4, 5, 6, 99, 99)), bytes((0, 1, 2, 3, 4, 5, 6))),
        ("bgr24", bytes((3, 2, 1, 6, 5, 4, 99, 99)), bytes((0, 1, 2, 3, 4, 5, 6))),
    ],
)
def test_rgb_and_bgr_encoding_ignore_valid_stride_padding(
    pixel_format: str, payload: bytes, expected: bytes
) -> None:
    snapshot = encode_png_snapshot(
        _frame(
            width=2,
            height=1,
            stride=8,
            pixel_format=pixel_format,
            payload=payload,
        )
    )

    width, height, color_type, scanlines = _decode_png(snapshot.png_bytes)
    assert (width, height, color_type) == (2, 1, 2)
    assert scanlines == expected


def test_downsample_is_nearest_neighbor_and_only_materializes_bounded_output() -> None:
    snapshot = encode_png_snapshot(
        _frame(
            width=4,
            height=2,
            stride=4,
            payload=bytes(range(8)),
        ),
        max_edge_px=2,
    )

    width, height, color_type, scanlines = _decode_png(snapshot.png_bytes)
    assert (width, height, color_type) == (2, 1, 0)
    assert scanlines == bytes((0, 0, 2))


def test_source_larger_than_hard_edge_is_downsampled_on_demand() -> None:
    snapshot = encode_png_snapshot(
        _frame(
            width=MAX_SNAPSHOT_EDGE_PX + 1,
            height=1,
            stride=MAX_SNAPSHOT_EDGE_PX + 1,
            payload=b"x" * (MAX_SNAPSHOT_EDGE_PX + 1),
        )
    )

    assert (snapshot.width, snapshot.height) == (MAX_SNAPSHOT_EDGE_PX, 1)


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (
            _frame(width=2, height=1, stride=2, pixel_format="rgb24", payload=b"xx"),
            "stride",
        ),
        (_frame(width=2, height=1, stride=2, payload=b"x"), "payload length"),
        (_frame(pixel_format="rgba32"), "supports only"),
        (_frame(width=0, height=0, stride=0, payload=b""), "positive dimensions"),
    ],
)
def test_encoder_rejects_invalid_layout_before_compression(
    frame: FrameEnvelope, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        encode_png_snapshot(frame)


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"max_edge_px": 0}, ValueError),
        ({"max_edge_px": MAX_SNAPSHOT_EDGE_PX + 1}, ValueError),
        ({"max_edge_px": True}, TypeError),
        ({"max_bytes": 0}, ValueError),
        ({"max_bytes": MAX_SNAPSHOT_BYTES + 1}, ValueError),
        ({"max_bytes": False}, TypeError),
        ({"sensitivity_class": "prohibited"}, ValueError),
        ({"sensitivity_class": "unknown"}, ValueError),
    ],
)
def test_encoder_rejects_invalid_limits_and_sensitivity(
    kwargs: dict[str, Any], error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        encode_png_snapshot(_frame(), **kwargs)


def test_encoder_aborts_explicitly_before_exceeding_requested_bytes() -> None:
    with pytest.raises(SnapshotTooLargeError, match="snapshot"):
        encode_png_snapshot(_frame(), max_bytes=57)


def test_compressor_failure_is_wrapped_without_touching_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = tuple(tmp_path.iterdir())

    def fail_compressor(*_args: Any, **_kwargs: Any) -> Any:
        raise zlib.error("injected")

    monkeypatch.setattr(png_module.zlib, "compressobj", fail_compressor)
    with pytest.raises(SnapshotEncodingError, match="compression failed"):
        encode_png_snapshot(_frame())

    assert tuple(tmp_path.iterdir()) == before


def test_encoder_requires_frame_envelope() -> None:
    with pytest.raises(TypeError, match="FrameEnvelope"):
        encode_png_snapshot(object())  # type: ignore[arg-type]
