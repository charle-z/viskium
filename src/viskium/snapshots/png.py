"""Deterministic, dependency-free PNG encoding for demanded snapshots."""

from __future__ import annotations

import binascii
import struct
import zlib
from collections.abc import Iterator

from viskium.core import FrameEnvelope
from viskium.core.contracts import SensitivityClass

from .contracts import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    PNG_SIGNATURE,
    SnapshotEnvelope,
)

_PNG_FIXED_BYTES = len(PNG_SIGNATURE) + 25 + 12 + 12


class SnapshotEncodingError(RuntimeError):
    """Raised when a valid snapshot cannot be encoded within its budget."""


class SnapshotTooLargeError(SnapshotEncodingError):
    """Raised before an encoded PNG can exceed its configured byte ceiling."""


def _positive_limit(value: object, field_name: str, *, hard_maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 1 <= value <= hard_maximum:
        raise ValueError(f"{field_name} must be between one and {hard_maximum}")
    return value


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    checksum = binascii.crc32(chunk_type)
    checksum = binascii.crc32(data, checksum) & 0xFFFF_FFFF
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", checksum)


def _target_dimensions(width: int, height: int, max_edge_px: int) -> tuple[int, int]:
    if max(width, height) <= max_edge_px:
        return width, height
    if width >= height:
        return max_edge_px, max(1, height * max_edge_px // width)
    return max(1, width * max_edge_px // height), max_edge_px


def _validate_frame(frame: FrameEnvelope) -> tuple[int, int]:
    if frame.pixel_format == "gray8":
        channels = 1
        color_type = 0
    elif frame.pixel_format in {"rgb24", "bgr24"}:
        channels = 3
        color_type = 2
    else:
        raise ValueError("snapshot encoder supports only gray8, rgb24, and bgr24")
    if frame.width <= 0 or frame.height <= 0:
        raise ValueError("snapshot frames require positive dimensions")
    row_bytes = frame.width * channels
    if frame.stride < row_bytes:
        raise ValueError("frame stride is smaller than its pixel row")
    expected_payload = frame.stride * frame.height
    if len(frame.payload) != expected_payload:
        raise ValueError("frame payload length does not match stride and height")
    return channels, color_type


def _scanlines(
    frame: FrameEnvelope,
    *,
    channels: int,
    target_width: int,
    target_height: int,
) -> Iterator[bytes]:
    for target_y in range(target_height):
        source_y = target_y * frame.height // target_height
        source_row = source_y * frame.stride
        row = bytearray(1 + target_width * channels)
        for target_x in range(target_width):
            source_x = target_x * frame.width // target_width
            source_offset = source_row + source_x * channels
            target_offset = 1 + target_x * channels
            if frame.pixel_format == "bgr24":
                row[target_offset] = frame.payload[source_offset + 2]
                row[target_offset + 1] = frame.payload[source_offset + 1]
                row[target_offset + 2] = frame.payload[source_offset]
            else:
                row[target_offset : target_offset + channels] = frame.payload[
                    source_offset : source_offset + channels
                ]
        yield bytes(row)


def encode_png_snapshot(
    frame: FrameEnvelope,
    *,
    sensitivity_class: SensitivityClass = "identifiable",
    max_edge_px: int = MAX_SNAPSHOT_EDGE_PX,
    max_bytes: int = MAX_SNAPSHOT_BYTES,
) -> SnapshotEnvelope:
    """Encode one demanded frame to an in-memory, metadata-free PNG."""

    if not isinstance(frame, FrameEnvelope):
        raise TypeError("frame must be a FrameEnvelope")
    edge_limit = _positive_limit(
        max_edge_px,
        "max_edge_px",
        hard_maximum=MAX_SNAPSHOT_EDGE_PX,
    )
    byte_limit = _positive_limit(max_bytes, "max_bytes", hard_maximum=MAX_SNAPSHOT_BYTES)
    if sensitivity_class not in {"public", "operational", "sensitive", "identifiable"}:
        raise ValueError("snapshot sensitivity must be allowed and non-prohibited")
    channels, color_type = _validate_frame(frame)
    target_width, target_height = _target_dimensions(frame.width, frame.height, edge_limit)

    compressed_budget = byte_limit - _PNG_FIXED_BYTES
    if compressed_budget < 0:
        raise SnapshotTooLargeError("snapshot byte ceiling is smaller than PNG framing")

    compressed_parts: list[bytes] = []
    compressed_size = 0
    try:
        compressor = zlib.compressobj(level=6, wbits=zlib.MAX_WBITS)
        for row in _scanlines(
            frame,
            channels=channels,
            target_width=target_width,
            target_height=target_height,
        ):
            part = compressor.compress(row)
            compressed_size += len(part)
            if compressed_size > compressed_budget:
                raise SnapshotTooLargeError(f"encoded snapshot exceeds {byte_limit} bytes")
            if part:
                compressed_parts.append(part)
        final_part = compressor.flush()
    except SnapshotEncodingError:
        raise
    except (MemoryError, zlib.error) as error:
        raise SnapshotEncodingError("PNG compression failed") from error

    compressed_size += len(final_part)
    if compressed_size > compressed_budget:
        raise SnapshotTooLargeError(f"encoded snapshot exceeds {byte_limit} bytes")
    if final_part:
        compressed_parts.append(final_part)
    compressed = b"".join(compressed_parts)

    header = struct.pack(">IIBBBBB", target_width, target_height, 8, color_type, 0, 0, 0)
    png_bytes = (
        PNG_SIGNATURE
        + _png_chunk(b"IHDR", header)
        + _png_chunk(b"IDAT", compressed)
        + _png_chunk(b"IEND", b"")
    )
    if len(png_bytes) > byte_limit:
        raise SnapshotTooLargeError(f"encoded snapshot exceeds {byte_limit} bytes")
    return SnapshotEnvelope(
        source_id=frame.source_id,
        stream_epoch=frame.stream_epoch,
        source_sequence=frame.sequence,
        received_monotonic_ns=frame.received_monotonic_ns,
        width=target_width,
        height=target_height,
        sensitivity_class=sensitivity_class,
        png_bytes=png_bytes,
    )


__all__ = ["SnapshotEncodingError", "SnapshotTooLargeError", "encode_png_snapshot"]
