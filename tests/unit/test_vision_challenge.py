import base64
import hashlib
import json
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.types import ImageContent, TextContent

from viskium.agent.mcp_server import (
    VERIFY_VISION_CHALLENGE_TOOL_V1,
    VISION_CHALLENGE_TOOL_V1,
    create_mcp_server,
)
from viskium.agent.vision_challenge import (
    OCR_TOKEN_ALPHABET,
    RELATION_VALUES,
    SHAPE_VALUES,
    VISION_CHALLENGE_HEIGHT,
    VISION_CHALLENGE_WIDTH,
    VisionChallengeCapacityError,
    VisionChallengeError,
    VisionChallengeService,
    VisionChallengeSpec,
    VisionChallengeStore,
    canonical_claims_sha256,
    generate_challenge_spec,
    generate_token,
    render_challenge,
)
from viskium.app import build_agent_application
from viskium.storage import initialize_data_root


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def _service(clock: Clock | None = None) -> tuple[VisionChallengeService, Clock]:
    selected_clock = Clock() if clock is None else clock
    store = VisionChallengeStore(clock=selected_clock, challenge_id_factory=lambda: "a" * 32)
    return VisionChallengeService(store=store), selected_clock


def _issue(service: VisionChallengeService) -> Any:
    return service.issue(VisionChallengeSpec("ACD347", "TRIANGLE", "CIRCLE", "LEFT_OF"))


def _claims(issued: Any, *, image_sha256: str | None = None) -> dict[str, str]:
    return {
        "challenge_id": issued.challenge_id,
        "image_sha256": issued.receipt.sha256 if image_sha256 is None else image_sha256,
        "token": issued.spec.token,
        "shape_a": issued.spec.shape_a,
        "shape_b": issued.spec.shape_b,
        "relation": issued.spec.relation,
    }


def test_renderer_is_deterministic_fixed_size_and_receipt_safe() -> None:
    for relation in RELATION_VALUES:
        for shape_a in SHAPE_VALUES:
            spec = VisionChallengeSpec("ACD347", shape_a, "CIRCLE", relation)
            first = render_challenge(spec)
            second = render_challenge(spec)
            assert first == second
            assert (first.width, first.height) == (VISION_CHALLENGE_WIDTH, VISION_CHALLENGE_HEIGHT)
            assert first.encoded_bytes <= 1 * 1024 * 1024
            assert first.png_bytes.startswith(b"\x89PNG\r\n\x1a\n")

    service, _ = _service()
    issued = _issue(service)
    receipt = json.dumps(issued.receipt.to_dict()).upper()
    assert all(
        value not in receipt for value in (issued.spec.token, *SHAPE_VALUES, *RELATION_VALUES)
    )
    assert not hasattr(service.store._records[issued.challenge_id], "png_bytes")


def test_token_and_spec_rng_are_injectable_and_bounded() -> None:
    class FixedRng:
        def randbelow(self, upper_bound: int) -> int:
            return upper_bound - 1

    token = generate_token(FixedRng())
    assert len(token) == 6
    assert token == OCR_TOKEN_ALPHABET[-1] * 6
    spec = generate_challenge_spec(FixedRng())
    assert spec.shape_a == SHAPE_VALUES[-1]
    assert spec.shape_b == SHAPE_VALUES[-1]
    assert spec.relation == RELATION_VALUES[-1]


def test_pass_fail_replay_hash_mismatch_and_canonical_claim_digest() -> None:
    service, _ = _service()
    issued = _issue(service)
    claims = _claims(issued)
    passed = service.verify(**claims)
    assert passed.outcome == "PASS"
    assert passed.attempts_used == 1
    assert passed.image_sha256 == issued.receipt.sha256
    assert passed.width == VISION_CHALLENGE_WIDTH
    assert passed.height == VISION_CHALLENGE_HEIGHT
    assert passed.claims_sha256 == canonical_claims_sha256(
        claims["image_sha256"],
        claims["token"],
        claims["shape_a"],
        claims["shape_b"],
        claims["relation"],
    )
    replay = service.verify(**claims)
    assert replay.outcome == "rejected"
    assert replay.image_sha256 == ""

    service, _ = _service()
    issued = _issue(service)
    wrong = _claims(issued)
    wrong["token"] = "AAAAAA"
    failed = service.verify(**wrong)
    assert failed.outcome == "FAIL"
    assert failed.image_sha256 == issued.receipt.sha256
    assert service.verify(**wrong).outcome == "rejected"

    service, _ = _service()
    issued = _issue(service)
    mismatch = service.verify(**_claims(issued, image_sha256="0" * 64))
    assert mismatch.outcome == "FAIL"


def test_missing_expired_used_and_invalid_are_uniform() -> None:
    service, clock = _service()
    issued = _issue(service)
    claims = _claims(issued)
    first = service.verify(**claims)
    used = service.verify(**claims)
    assert first.outcome == "PASS"
    assert used.outcome == "rejected"

    clock.value = 121.0
    expired = service.verify(**claims)
    missing = service.verify(**{**claims, "challenge_id": "b" * 32})
    invalid = service.verify(**{**claims, "token": "bad"})
    assert expired.outcome == missing.outcome == invalid.outcome == "rejected"
    assert expired.width == missing.width == invalid.width == 0
    assert expired.height == missing.height == invalid.height == 0
    assert expired.byte_count == missing.byte_count == invalid.byte_count == 0


def test_valid_id_with_malformed_claim_is_fail_then_consumed() -> None:
    service, _ = _service()
    issued = _issue(service)
    malformed = {**_claims(issued), "token": "not-a-token"}
    failed = service.verify(**malformed)
    assert failed.outcome == "FAIL"
    assert failed.attempts_used == 1
    assert failed.image_sha256 == issued.receipt.sha256
    assert service.verify(**_claims(issued)).outcome == "rejected"


def test_ttl_worker_removes_expected_claims_proactively() -> None:
    store = VisionChallengeStore(ttl_seconds=0.08)
    service = VisionChallengeService(store=store)
    _issue(service)
    assert store.size == 1
    deadline = time.monotonic() + 1.0
    while store.size and time.monotonic() < deadline:
        time.sleep(0.02)
    assert store.size == 0
    service.close()
    assert not store._expiry_thread.is_alive()


def test_bounded_validation_and_clean_store_close() -> None:
    invalid_specs = [
        ("acd347", "TRIANGLE", "CIRCLE", "LEFT_OF"),
        ("ACD347", "OCTAGON", "CIRCLE", "LEFT_OF"),
        ("ACD347", "TRIANGLE", "OCTAGON", "LEFT_OF"),
        ("ACD347", "TRIANGLE", "CIRCLE", "NEAR"),
    ]
    for values in invalid_specs:
        try:
            VisionChallengeSpec(*values)  # type: ignore[arg-type]
        except VisionChallengeError:
            pass
        else:
            raise AssertionError("invalid spec was accepted")

    with pytest.raises(VisionChallengeError):
        generate_token(lambda upper_bound: upper_bound)
    with pytest.raises(TypeError):
        render_challenge(object())  # type: ignore[arg-type]

    store = VisionChallengeStore()
    store.close()
    store.close()
    assert store.size == 0
    with pytest.raises(VisionChallengeError):
        store.issue(
            spec=VisionChallengeSpec("ACD347", "TRIANGLE", "CIRCLE", "LEFT_OF"),
            image_sha256="0" * 64,
            width=512,
            height=384,
            byte_count=1,
        )


def test_store_cap_cleanup_and_atomic_single_winner() -> None:
    clock = Clock()
    ids = iter(f"{value:032x}" for value in range(32))
    store = VisionChallengeStore(clock=clock, challenge_id_factory=lambda: next(ids), max_active=32)
    service = VisionChallengeService(store=store)
    for _ in range(32):
        _issue(service)
    try:
        _issue(service)
    except VisionChallengeCapacityError:
        pass
    else:
        raise AssertionError("challenge cap was not enforced")
    clock.value = 121.0
    assert store.size == 0

    service, _ = _service()
    issued = _issue(service)
    claims = _claims(issued)
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: service.verify(**claims), range(8)))
    assert sum(result.outcome == "PASS" for result in results) == 1
    assert sum(result.outcome == "rejected" for result in results) == 7


def test_mcp_challenge_and_verify_have_visible_safe_wire_shapes(tmp_path: Path) -> None:
    root = initialize_data_root(tmp_path / "data").root
    server = create_mcp_server(build_agent_application(root).service)

    async def scenario() -> tuple[Any, Any]:
        async with Client(server) as client:
            challenge = await client.call_tool(VISION_CHALLENGE_TOOL_V1)
            receipt = json.loads(challenge.content[1].text)
            assert isinstance(challenge.content[0], ImageContent)
            assert isinstance(challenge.content[1], TextContent)
            image = base64.b64decode(challenge.content[0].data, validate=True)
            assert hashlib.sha256(image).hexdigest() == receipt["sha256"]
            assert challenge.content[1].text == json.dumps(
                challenge.structured_content,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            chunks = []
            offset = 8
            while offset < len(image):
                length = struct.unpack(">I", image[offset : offset + 4])[0]
                chunks.append(image[offset + 4 : offset + 8])
                offset += 12 + length
            assert chunks == [b"IHDR", b"IDAT", b"IEND"]
            malformed = await client.call_tool(
                VERIFY_VISION_CHALLENGE_TOOL_V1,
                {
                    "challenge_id": receipt["challenge_id"],
                    "image_sha256": receipt["sha256"],
                    "token": "bad",
                    "shape_a": "CIRCLE",
                    "shape_b": "SQUARE",
                    "relation": "LEFT_OF",
                },
            )
            replay = await client.call_tool(
                VERIFY_VISION_CHALLENGE_TOOL_V1,
                {
                    "challenge_id": receipt["challenge_id"],
                    "image_sha256": receipt["sha256"],
                    "token": "AAAAAA",
                    "shape_a": "CIRCLE",
                    "shape_b": "SQUARE",
                    "relation": "LEFT_OF",
                },
            )
            assert malformed.structured_content["outcome"] == "FAIL"
            assert replay.structured_content["outcome"] == "rejected"
            return challenge, malformed

    challenge, verify = anyio.run(scenario)
    assert len(challenge.content) == 2
    assert verify.structured_content["outcome"] in {"PASS", "FAIL", "rejected"}
    assert "expected" not in verify.content[0].text.lower()
    assert "token" not in json.loads(challenge.content[1].text)
