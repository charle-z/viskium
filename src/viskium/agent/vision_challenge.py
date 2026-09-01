"""Ephemeral visual challenge protocol for testing image delivery."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Lock, Thread, current_thread
from typing import Final, Literal, Protocol, cast

from viskium.core import FrameEnvelope
from viskium.core.serialization import bounded_canonical_json_bytes
from viskium.snapshots import SnapshotEnvelope, encode_png_snapshot

VISION_CHALLENGE_RESULT_CONTRACT_V1: Final = "urn:viskium:mcp:vision-challenge:1"
VISION_PROOF_RESULT_CONTRACT_V1: Final = "urn:viskium:mcp:vision-proof:1"
VISION_CHALLENGE_TOOL_V1: Final = "viskium_vision_challenge_v1"
VERIFY_VISION_CHALLENGE_TOOL_V1: Final = "viskium_verify_vision_challenge_v1"
VISION_CHALLENGE_WIDTH: Final = 512
VISION_CHALLENGE_HEIGHT: Final = 384
VISION_CHALLENGE_MAX_BYTES: Final = 1 * 1024 * 1024
VISION_CHALLENGE_TTL_SECONDS: Final = 120.0
VISION_CHALLENGE_MAX_ACTIVE: Final = 32
VISION_CHALLENGE_TOKEN_LENGTH: Final = 6
# Characters with common OCR confusions removed (0/O, 1/I, 2/Z, 5/S, 8/B).
OCR_TOKEN_ALPHABET: Final = "ACDEFGHJKLMNPQRTUVWXY3479"
SHAPE_VALUES: Final = ("CIRCLE", "TRIANGLE", "SQUARE", "DIAMOND", "STAR")
RELATION_VALUES: Final = ("LEFT_OF", "RIGHT_OF", "ABOVE", "BELOW")

type ShapeName = Literal["CIRCLE", "TRIANGLE", "SQUARE", "DIAMOND", "STAR"]
type RelationName = Literal["LEFT_OF", "RIGHT_OF", "ABOVE", "BELOW"]


class RandomSource(Protocol):
    def randbelow(self, upper_bound: int, /) -> int: ...


class VisionChallengeError(ValueError):
    """Base class for bounded challenge input errors."""


class VisionChallengeCapacityError(VisionChallengeError):
    """The bounded in-memory challenge cap has been reached."""


def _validate_token(value: object) -> str:
    if (
        type(value) is not str
        or len(value) != VISION_CHALLENGE_TOKEN_LENGTH
        or not value.isascii()
        or value != value.upper()
        or any(char not in OCR_TOKEN_ALPHABET for char in value)
    ):
        raise VisionChallengeError("invalid challenge claims")
    return value


def _validate_shape(value: object) -> ShapeName:
    if type(value) is not str or not value.isascii() or value not in SHAPE_VALUES:
        raise VisionChallengeError("invalid challenge claims")
    return cast(ShapeName, value)


def _validate_relation(value: object) -> RelationName:
    if type(value) is not str or not value.isascii() or value not in RELATION_VALUES:
        raise VisionChallengeError("invalid challenge claims")
    return cast(RelationName, value)


def _validate_hash(value: object) -> str:
    if type(value) is not str or not value.isascii() or len(value) != 64:
        raise VisionChallengeError("invalid challenge claims")
    try:
        int(value, 16)
    except ValueError as error:
        raise VisionChallengeError("invalid challenge claims") from error
    return value.lower()


def _validate_id(value: object) -> str:
    if type(value) is not str or not value.isascii() or len(value) != 32:
        raise VisionChallengeError("invalid challenge claims")
    try:
        int(value, 16)
    except ValueError as error:
        raise VisionChallengeError("invalid challenge claims") from error
    return value.lower()


def _randbelow(
    rng: RandomSource | Callable[[int], int] | None,
    upper_bound: int,
) -> int:
    value = (
        secrets.randbelow(upper_bound)
        if rng is None
        else (rng(upper_bound) if callable(rng) else rng.randbelow(upper_bound))
    )
    if type(value) is not int or not 0 <= value < upper_bound:
        raise VisionChallengeError("random source returned an invalid value")
    return value


def generate_token(rng: RandomSource | Callable[[int], int] | None = None) -> str:
    """Generate a six-character token with a CSPRNG unless a test RNG is injected."""

    return "".join(
        OCR_TOKEN_ALPHABET[_randbelow(rng, len(OCR_TOKEN_ALPHABET))]
        for _ in range(VISION_CHALLENGE_TOKEN_LENGTH)
    )


@dataclass(frozen=True, slots=True)
class VisionChallengeSpec:
    token: str
    shape_a: ShapeName
    shape_b: ShapeName
    relation: RelationName

    def __post_init__(self) -> None:
        _validate_token(self.token)
        _validate_shape(self.shape_a)
        _validate_shape(self.shape_b)
        _validate_relation(self.relation)


def generate_challenge_spec(
    rng: RandomSource | Callable[[int], int] | None = None,
) -> VisionChallengeSpec:
    return VisionChallengeSpec(
        token=generate_token(rng),
        shape_a=SHAPE_VALUES[_randbelow(rng, len(SHAPE_VALUES))],
        shape_b=SHAPE_VALUES[_randbelow(rng, len(SHAPE_VALUES))],
        relation=RELATION_VALUES[_randbelow(rng, len(RELATION_VALUES))],
    )


_GLYPHS: dict[str, tuple[str, ...]] = {
    "A": ("01110", "10001", "10001", "11111", "10001", "10001", "10001"),
    "B": ("11110", "10001", "10001", "11110", "10001", "10001", "11110"),
    "C": ("01111", "10000", "10000", "10000", "10000", "10000", "01111"),
    "D": ("11110", "10001", "10001", "10001", "10001", "10001", "11110"),
    "E": ("11111", "10000", "10000", "11110", "10000", "10000", "11111"),
    "F": ("11111", "10000", "10000", "11110", "10000", "10000", "10000"),
    "G": ("01111", "10000", "10000", "10111", "10001", "10001", "01111"),
    "H": ("10001", "10001", "10001", "11111", "10001", "10001", "10001"),
    "J": ("00111", "00010", "00010", "00010", "10010", "10010", "01100"),
    "K": ("10001", "10010", "10100", "11000", "10100", "10010", "10001"),
    "L": ("10000", "10000", "10000", "10000", "10000", "10000", "11111"),
    "M": ("10001", "11011", "10101", "10101", "10001", "10001", "10001"),
    "N": ("10001", "11001", "10101", "10011", "10001", "10001", "10001"),
    "P": ("11110", "10001", "10001", "11110", "10000", "10000", "10000"),
    "Q": ("01110", "10001", "10001", "10001", "10101", "10010", "01101"),
    "R": ("11110", "10001", "10001", "11110", "10100", "10010", "10001"),
    "S": ("01111", "10000", "10000", "01110", "00001", "00001", "11110"),
    "T": ("11111", "00100", "00100", "00100", "00100", "00100", "00100"),
    "U": ("10001", "10001", "10001", "10001", "10001", "10001", "01110"),
    "V": ("10001", "10001", "10001", "10001", "10001", "01010", "00100"),
    "W": ("10001", "10001", "10001", "10101", "10101", "11011", "10001"),
    "X": ("10001", "10001", "01010", "00100", "01010", "10001", "10001"),
    "Y": ("10001", "10001", "01010", "00100", "00100", "00100", "00100"),
    "Z": ("11111", "00001", "00010", "00100", "01000", "10000", "11111"),
    "2": ("01110", "10001", "00001", "00010", "00100", "01000", "11111"),
    "3": ("11110", "00001", "00001", "01110", "00001", "00001", "11110"),
    "4": ("00010", "00110", "01010", "10010", "11111", "00010", "00010"),
    "5": ("11111", "10000", "10000", "11110", "00001", "00001", "11110"),
    "6": ("01110", "10000", "10000", "11110", "10001", "10001", "01110"),
    "7": ("11111", "00001", "00010", "00100", "01000", "01000", "01000"),
    "8": ("01110", "10001", "10001", "01110", "10001", "10001", "01110"),
    "9": ("01110", "10001", "10001", "01111", "00001", "00001", "01110"),
}


def _rect(
    canvas: bytearray, x0: int, y0: int, x1: int, y1: int, color: tuple[int, int, int]
) -> None:
    for y in range(max(0, y0), min(VISION_CHALLENGE_HEIGHT, y1)):
        start = (y * VISION_CHALLENGE_WIDTH + max(0, x0)) * 3
        end = (y * VISION_CHALLENGE_WIDTH + min(VISION_CHALLENGE_WIDTH, x1)) * 3
        canvas[start:end] = bytes(color) * ((end - start) // 3)


def _pixel(canvas: bytearray, x: int, y: int, color: tuple[int, int, int]) -> None:
    if 0 <= x < VISION_CHALLENGE_WIDTH and 0 <= y < VISION_CHALLENGE_HEIGHT:
        offset = (y * VISION_CHALLENGE_WIDTH + x) * 3
        canvas[offset : offset + 3] = bytes(color)


def _circle(canvas: bytearray, cx: int, cy: int, radius: int, color: tuple[int, int, int]) -> None:
    for y in range(cy - radius, cy + radius + 1):
        for x in range(cx - radius, cx + radius + 1):
            if (x - cx) ** 2 + (y - cy) ** 2 <= radius**2:
                _pixel(canvas, x, y, color)


def _polygon(
    canvas: bytearray, points: tuple[tuple[int, int], ...], color: tuple[int, int, int]
) -> None:
    low_y = max(0, min(y for _, y in points))
    high_y = min(VISION_CHALLENGE_HEIGHT - 1, max(y for _, y in points))
    for y in range(low_y, high_y + 1):
        crossings: list[int] = []
        for index, (x0, y0) in enumerate(points):
            x1, y1 = points[(index + 1) % len(points)]
            if (y0 <= y < y1) or (y1 <= y < y0):
                crossings.append(x0 + (y - y0) * (x1 - x0) // (y1 - y0))
        crossings.sort()
        for start in range(0, len(crossings) - 1, 2):
            _rect(canvas, crossings[start], y, crossings[start + 1] + 1, y + 1, color)


def _shape(
    canvas: bytearray, name: ShapeName, cx: int, cy: int, color: tuple[int, int, int]
) -> None:
    radius = 58
    if name == "CIRCLE":
        _circle(canvas, cx, cy, radius, color)
    elif name == "SQUARE":
        _rect(canvas, cx - radius, cy - radius, cx + radius + 1, cy + radius + 1, color)
    elif name == "TRIANGLE":
        _polygon(
            canvas,
            ((cx, cy - radius), (cx - radius, cy + radius), (cx + radius, cy + radius)),
            color,
        )
    elif name == "DIAMOND":
        _polygon(
            canvas,
            ((cx, cy - radius), (cx + radius, cy), (cx, cy + radius), (cx - radius, cy)),
            color,
        )
    else:
        _polygon(
            canvas,
            (
                (cx, cy - radius),
                (cx + 20, cy - 20),
                (cx + radius, cy - 20),
                (cx + 28, cy + 10),
                (cx + 38, cy + radius),
                (cx, cy + 28),
                (cx - 38, cy + radius),
                (cx - 28, cy + 10),
                (cx - radius, cy - 20),
                (cx - 20, cy - 20),
            ),
            color,
        )


def _text(
    canvas: bytearray, value: str, x: int, y: int, scale: int, color: tuple[int, int, int]
) -> None:
    for character in value:
        for row, bitmap in enumerate(_GLYPHS[character]):
            for column, filled in enumerate(bitmap):
                if filled == "1":
                    _rect(
                        canvas,
                        x + column * scale,
                        y + row * scale,
                        x + (column + 1) * scale,
                        y + (row + 1) * scale,
                        color,
                    )
        x += 6 * scale


def render_challenge(spec: VisionChallengeSpec) -> SnapshotEnvelope:
    """Render deterministic BGR24 pixels and encode them with Viskium's PNG encoder."""

    if not isinstance(spec, VisionChallengeSpec):
        raise TypeError("spec must be a VisionChallengeSpec")
    canvas = bytearray(bytes((238, 238, 238)) * (VISION_CHALLENGE_WIDTH * VISION_CHALLENGE_HEIGHT))
    border = (64, 64, 64)
    _rect(canvas, 0, 0, VISION_CHALLENGE_WIDTH, 8, border)
    _rect(
        canvas,
        0,
        VISION_CHALLENGE_HEIGHT - 8,
        VISION_CHALLENGE_WIDTH,
        VISION_CHALLENGE_HEIGHT,
        border,
    )
    _rect(canvas, 0, 0, 8, VISION_CHALLENGE_HEIGHT, border)
    _rect(
        canvas,
        VISION_CHALLENGE_WIDTH - 8,
        0,
        VISION_CHALLENGE_WIDTH,
        VISION_CHALLENGE_HEIGHT,
        border,
    )
    _text(
        canvas,
        spec.token,
        (VISION_CHALLENGE_WIDTH - len(spec.token) * 36) // 2,
        28,
        6,
        (20, 20, 20),
    )
    positions = {
        "LEFT_OF": ((150, 225), (362, 225)),
        "RIGHT_OF": ((362, 225), (150, 225)),
        "ABOVE": ((256, 135), (256, 290)),
        "BELOW": ((256, 290), (256, 135)),
    }
    (ax, ay), (bx, by) = positions[spec.relation]
    _shape(canvas, spec.shape_a, ax, ay, (45, 70, 220))
    _shape(canvas, spec.shape_b, bx, by, (220, 90, 45))
    _text(canvas, "A", ax - 9, ay - 13, 3, (255, 255, 255))
    _text(canvas, "B", bx - 9, by - 13, 3, (255, 255, 255))
    frame = FrameEnvelope(
        source_id="vision-challenge",
        stream_epoch=1,
        sequence=1,
        received_monotonic_ns=0,
        payload=bytes(canvas),
        width=VISION_CHALLENGE_WIDTH,
        height=VISION_CHALLENGE_HEIGHT,
        pixel_format="bgr24",
        stride=VISION_CHALLENGE_WIDTH * 3,
    )
    return encode_png_snapshot(
        frame,
        sensitivity_class="public",
        max_edge_px=VISION_CHALLENGE_WIDTH,
        max_bytes=VISION_CHALLENGE_MAX_BYTES,
    )


build_challenge_image = render_challenge


def _claims_document(
    image_sha256: str, token: str, shape_a: str, shape_b: str, relation: str
) -> dict[str, str]:
    return {
        "image_sha256": image_sha256,
        "relation": relation,
        "shape_a": shape_a,
        "shape_b": shape_b,
        "token": token,
    }


def canonical_claims_sha256(
    image_sha256: str, token: str, shape_a: str, shape_b: str, relation: str
) -> str:
    encoded = bounded_canonical_json_bytes(
        _claims_document(image_sha256, token, shape_a, shape_b, relation), max_bytes=512
    )
    if encoded is None:  # pragma: no cover
        raise VisionChallengeError("invalid challenge claims")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class VisionChallengeReceipt:
    challenge_id: str
    sha256: str
    width: int
    height: int
    byte_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "contract": VISION_CHALLENGE_RESULT_CONTRACT_V1,
            "challenge_id": self.challenge_id,
            "mime_type": "image/png",
            "width": self.width,
            "height": self.height,
            "byte_count": self.byte_count,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class IssuedVisionChallenge:
    challenge_id: str
    snapshot: SnapshotEnvelope
    receipt: VisionChallengeReceipt
    spec: VisionChallengeSpec


@dataclass(frozen=True, slots=True)
class VisionVerification:
    outcome: Literal["PASS", "FAIL", "rejected"]
    challenge_id: str
    image_sha256: str
    width: int
    height: int
    byte_count: int
    attempts_used: int
    claims_sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "contract": VISION_PROOF_RESULT_CONTRACT_V1,
            "outcome": self.outcome,
            "challenge_id": self.challenge_id,
            "image_sha256": self.image_sha256,
            "width": self.width,
            "height": self.height,
            "byte_count": self.byte_count,
            "attempts_used": self.attempts_used,
            "claims_sha256": self.claims_sha256,
        }


@dataclass(slots=True)
class _StoredChallenge:
    expected_token: str
    expected_shape_a: ShapeName
    expected_shape_b: ShapeName
    expected_relation: RelationName
    image_sha256: str
    width: int
    height: int
    byte_count: int
    expires_at: float
    used: bool = False


class VisionChallengeStore:
    """Bounded thread-safe RAM store; records contain no image bytes."""

    def __init__(
        self,
        *,
        ttl_seconds: float = VISION_CHALLENGE_TTL_SECONDS,
        max_active: int = VISION_CHALLENGE_MAX_ACTIVE,
        clock: Callable[[], float] = time.monotonic,
        challenge_id_factory: Callable[[], str] = lambda: secrets.token_hex(16),
    ) -> None:
        if type(ttl_seconds) not in (int, float) or not 0 < ttl_seconds <= 300:
            raise ValueError("ttl_seconds must be positive and bounded")
        if type(max_active) is not int or not 1 <= max_active <= VISION_CHALLENGE_MAX_ACTIVE:
            raise ValueError("max_active is outside the bounded cap")
        self._ttl_seconds, self._max_active, self._clock = float(ttl_seconds), max_active, clock
        self._challenge_id_factory = challenge_id_factory
        self._records: dict[str, _StoredChallenge] = {}
        self._lock = Lock()
        self._stop_event = Event()
        self._expiry_thread = Thread(
            target=VisionChallengeStore._expiry_worker,
            args=(weakref.ref(self), self._stop_event, self._ttl_seconds),
            name="viskium-vision-challenge-expiry",
            daemon=True,
        )
        self._expiry_thread.start()

    @property
    def size(self) -> int:
        with self._lock:
            self._cleanup_locked(self._clock())
            return len(self._records)

    @property
    def ttl_seconds(self) -> float:
        return self._ttl_seconds

    def _cleanup_locked(self, now: float) -> None:
        for challenge_id in tuple(self._records):
            if self._records[challenge_id].expires_at <= now:
                del self._records[challenge_id]

    @staticmethod
    def _expiry_worker(
        store_ref: weakref.ReferenceType[VisionChallengeStore],
        stop_event: Event,
        ttl_seconds: float,
    ) -> None:
        """Expire records proactively without keeping the store alive."""

        interval = min(max(ttl_seconds / 4.0, 0.05), 1.0)
        while not stop_event.wait(interval):
            store = store_ref()
            if store is None:
                return
            with store._lock:
                store._cleanup_locked(store._clock())

    def close(self) -> None:
        """Stop the bounded daemon and clear all expected claims."""

        self._stop_event.set()
        if self._expiry_thread is not current_thread():
            self._expiry_thread.join(timeout=1.0)
        with self._lock:
            self._records.clear()

    def __enter__(self) -> VisionChallengeStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def issue(
        self,
        *,
        spec: VisionChallengeSpec,
        image_sha256: str,
        width: int,
        height: int,
        byte_count: int,
    ) -> str:
        if not isinstance(spec, VisionChallengeSpec):
            raise TypeError("spec must be a VisionChallengeSpec")
        image_sha256 = _validate_hash(image_sha256)
        if type(width) is not int or type(height) is not int or type(byte_count) is not int:
            raise TypeError("challenge image metadata must be integers")
        if not 1 <= width <= VISION_CHALLENGE_WIDTH or not 1 <= height <= VISION_CHALLENGE_HEIGHT:
            raise VisionChallengeError("challenge image dimensions are bounded")
        if not 1 <= byte_count <= VISION_CHALLENGE_MAX_BYTES:
            raise VisionChallengeError("challenge image size is bounded")
        with self._lock:
            now = self._clock()
            self._cleanup_locked(now)
            if self._stop_event.is_set():
                raise VisionChallengeError("challenge store is closed")
            if len(self._records) >= self._max_active:
                raise VisionChallengeCapacityError("challenge capacity is exhausted")
            challenge_id = _validate_id(self._challenge_id_factory())
            if challenge_id in self._records:
                raise VisionChallengeCapacityError("challenge id collision")
            self._records[challenge_id] = _StoredChallenge(
                spec.token,
                spec.shape_a,
                spec.shape_b,
                spec.relation,
                image_sha256,
                width,
                height,
                byte_count,
                now + self._ttl_seconds,
            )
            return challenge_id

    create = issue

    def verify(
        self,
        *,
        challenge_id: object,
        image_sha256: object,
        token: object,
        shape_a: object,
        shape_b: object,
        relation: object,
    ) -> VisionVerification:
        safe_id: str = ""
        safe_hash: str = ""
        safe_token: str = ""
        safe_a: str = ""
        safe_b: str = ""
        safe_relation: str = ""
        try:
            safe_id = _validate_id(challenge_id)
        except VisionChallengeError:
            safe_id = ""
        try:
            safe_hash = _validate_hash(image_sha256)
        except VisionChallengeError:
            safe_hash = ""
        try:
            safe_token = _validate_token(token)
        except VisionChallengeError:
            safe_token = ""
        try:
            safe_a = _validate_shape(shape_a)
        except VisionChallengeError:
            safe_a = ""
        try:
            safe_b = _validate_shape(shape_b)
        except VisionChallengeError:
            safe_b = ""
        try:
            safe_relation = _validate_relation(relation)
        except VisionChallengeError:
            safe_relation = ""
        claims_hash = canonical_claims_sha256(safe_hash, safe_token, safe_a, safe_b, safe_relation)
        with self._lock:
            self._cleanup_locked(self._clock())
            record = self._records.get(safe_id)
            if record is None or record.used:
                return VisionVerification("rejected", safe_id, "", 0, 0, 0, 1, claims_hash)
            record.used = True
            comparisons = [
                hmac.compare_digest(safe_token, record.expected_token),
                hmac.compare_digest(safe_a, record.expected_shape_a),
                hmac.compare_digest(safe_b, record.expected_shape_b),
                hmac.compare_digest(safe_relation, record.expected_relation),
                hmac.compare_digest(safe_hash, record.image_sha256),
            ]
            return VisionVerification(
                "PASS" if all(comparisons) else "FAIL",
                safe_id,
                record.image_sha256,
                record.width,
                record.height,
                record.byte_count,
                1,
                claims_hash,
            )


class VisionChallengeService:
    def __init__(
        self,
        *,
        store: VisionChallengeStore | None = None,
        rng: RandomSource | Callable[[int], int] | None = None,
    ) -> None:
        self.store = VisionChallengeStore() if store is None else store
        self._rng = rng

    def issue(self, spec: VisionChallengeSpec | None = None) -> IssuedVisionChallenge:
        selected = generate_challenge_spec(self._rng) if spec is None else spec
        snapshot = render_challenge(selected)
        digest = hashlib.sha256(snapshot.png_bytes).hexdigest()
        challenge_id = self.store.issue(
            spec=selected,
            image_sha256=digest,
            width=snapshot.width,
            height=snapshot.height,
            byte_count=snapshot.encoded_bytes,
        )
        receipt = VisionChallengeReceipt(
            challenge_id, digest, snapshot.width, snapshot.height, snapshot.encoded_bytes
        )
        return IssuedVisionChallenge(challenge_id, snapshot, receipt, selected)

    create = issue

    def verify(self, **claims: object) -> VisionVerification:
        return self.store.verify(**claims)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> VisionChallengeService:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = [
    "OCR_TOKEN_ALPHABET",
    "RELATION_VALUES",
    "SHAPE_VALUES",
    "VERIFY_VISION_CHALLENGE_TOOL_V1",
    "VISION_CHALLENGE_HEIGHT",
    "VISION_CHALLENGE_MAX_ACTIVE",
    "VISION_CHALLENGE_MAX_BYTES",
    "VISION_CHALLENGE_RESULT_CONTRACT_V1",
    "VISION_CHALLENGE_TOKEN_LENGTH",
    "VISION_CHALLENGE_TOOL_V1",
    "VISION_CHALLENGE_TTL_SECONDS",
    "VISION_CHALLENGE_WIDTH",
    "VISION_PROOF_RESULT_CONTRACT_V1",
    "IssuedVisionChallenge",
    "VisionChallengeCapacityError",
    "VisionChallengeError",
    "VisionChallengeReceipt",
    "VisionChallengeService",
    "VisionChallengeSpec",
    "VisionChallengeStore",
    "VisionVerification",
    "build_challenge_image",
    "canonical_claims_sha256",
    "generate_challenge_spec",
    "generate_token",
    "render_challenge",
]
